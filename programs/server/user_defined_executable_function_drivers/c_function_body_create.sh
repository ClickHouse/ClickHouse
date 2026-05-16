#!/bin/sh
# Wrapper for the C-body driver create_command.
# Delegates to c_function_body_driver.py with the "create" subcommand,
# forwarding all flags and stdin.
exec "$(dirname "$0")/c_function_body_driver.py" create "$@"
