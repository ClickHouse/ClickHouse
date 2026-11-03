#!/bin/sh
# Wrapper for the `UnsafeC` C-body driver `drop_command`.
exec "$(dirname "$0")/c_driver_common.py" --runtime unsafe drop "$@"
