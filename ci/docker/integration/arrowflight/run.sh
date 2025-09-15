#!/bin/bash

# Stop background processes when this script exits.
trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM EXIT

python flight_server.py --port 5005 &
python flight_server.py --port 5006 --username=test_user --password=test_password &

# Wait for signals.
wait
