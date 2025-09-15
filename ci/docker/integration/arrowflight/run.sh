#!/bin/bash

# Stop background processes when this script exits.
trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM EXIT

python flight_server.py --port 5005 &
python flight_server.py --port 5006 --username="admin" --password="ClickHouse_ArrowFlight_P@ssw0rd" &

# Wait for signals.
wait
