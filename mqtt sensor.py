import json
import random
import time
from datetime import datetime
import paho.mqtt.client as mqtt

# MQTT Broker (public free broker)
BROKER = "test.mosquitto.org"
PORT = 1883
TOPIC = "scitech/assignment/iot"

# Output file
OUTPUT_FILE = "sensor_data.json"

# Callback when connected
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to MQTT Broker")
        client.subscribe(TOPIC)
    else:
        print(f"Connection failed with code {rc}")

# Callback when message received
def on_message(client, userdata, msg):
    payload = msg.payload.decode()
    print(f" Received: {payload}")
    try:
        data = json.loads(payload)
        with open(OUTPUT_FILE, "a") as f:
            json.dump(data, f)
            f.write("\n")
    except Exception as e:
        print(" Error saving:", e)

# Generate random sensor data
def generate_sensor_data():
    return {
        "timestamp": datetime.now().isoformat(),
        "temperature": round(random.uniform(20.0, 35.0), 2),
        "energy_usage": round(random.uniform(100.0, 500.0), 2)
    }

def main():
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(BROKER, PORT, keepalive=60)
    client.loop_start()

    try:
        while True:
            data = generate_sensor_data()
            payload = json.dumps(data)
            client.publish(TOPIC, payload)
            print(f" Published: {payload}")
            time.sleep(5)
    except KeyboardInterrupt:
        print("\n Stopped by user")
    finally:
        client.loop_stop()
        client.disconnect()

if __name__ == "__main__":
    main()
