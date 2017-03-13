#!/usr/bin/env bash

# Accepts a directory with chunks as argument. Constantly shows the list of active parts and the number of all parts.

watch "ls $1 | grep -Pc '^[0-9]{8}_[0-9]{8}_'; ls $1 | active_parts.py | grep -Pc '^[0-9]{8}_[0-9]{8}_'; ls $1 | active_parts.py"
