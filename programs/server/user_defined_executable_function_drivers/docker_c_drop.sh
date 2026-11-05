#!/bin/sh
# Wrapper for the `DockerC` C-body driver `drop_command`.
exec "$(dirname "$0")/c_driver_common.py" --runtime docker drop "$@"
