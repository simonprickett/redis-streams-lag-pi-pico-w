import network
import secrets
import time
from machine import Pin
from picoredis import Redis

# A list of Pins, representing our 10 LEDs.
leds = [
    Pin(0, Pin.OUT),
    Pin(1, Pin.OUT),
    Pin(2, Pin.OUT),
    Pin(3, Pin.OUT),
    Pin(4, Pin.OUT),
    Pin(5, Pin.OUT),
    Pin(6, Pin.OUT),
    Pin(7, Pin.OUT),
    Pin(8, Pin.OUT),
    Pin(9, Pin.OUT)
]

STREAM_KEY = "jobs"
CONSUMER_GROUP = "staff"

# Turn all the LEDs off...
for led in leds:
    led.value(0)

wlan = network.WLAN(network.STA_IF)
wlan.active(True)
wlan.connect(secrets.WIFI_SSID, secrets.WIFI_PASSWORD)

while not wlan.isconnected() and wlan.status() >= 0:
    print("Connecting to wifi...")
    time.sleep(0.5)

# We should now have a network connection so let's connect to Redis...
print(f"Connecting to Redis at {secrets.REDIS_HOST}:{secrets.REDIS_PORT}...")
r = Redis(host = secrets.REDIS_HOST, port = secrets.REDIS_PORT)

if len(secrets.REDIS_PASSWORD) > 0:
    # Authenticate to Redis.
    r.auth(secrets.REDIS_PASSWORD)

while True:
    response = r.xinfo("groups", STREAM_KEY)

    for group_info in response:
        # Get the right consumer group as there may be multiple.
        # Redis client returns bytes so need to decode to UTF-8 string to compare.
        if (group_info[1].decode('utf-8') == CONSUMER_GROUP):
            # Get the lag value from the consumer group.
            current_lag = group_info[11]

            # Let's have each LED represent a 2 entry lag.
            leds_to_show = current_lag // 2

            # If the lag requires more LEDs than we have, cap it at the max.
            if leds_to_show > 10:
                leds_to_show = 10

            # Turn on all the LEDs we need to show the current lag.
            for led in leds[:leds_to_show]:
                led.value(1)

            # Turn off any remaining LEDs.
            for led in leds[leds_to_show:]:
                led.value(0)

    time.sleep(2)