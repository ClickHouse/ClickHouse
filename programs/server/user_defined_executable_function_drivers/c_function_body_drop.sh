#!/bin/sh
# Wrapper for the C-body driver drop_command.
exec "$(dirname "$0")/c_function_body_driver.py" drop "$@"
