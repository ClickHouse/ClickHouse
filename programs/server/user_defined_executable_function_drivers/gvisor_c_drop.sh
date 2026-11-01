#!/bin/sh
# Wrapper for the `GVisorC` C-body driver `drop_command`.
exec "$(dirname "$0")/c_driver_common.py" --runtime gvisor drop "$@"
