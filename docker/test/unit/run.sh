#!/bin/bash

set -x
# Need to keep error from tests after `tee`. Otherwise we don't alert on asan errors
set -o pipefail
set -e

if [ "$#" -ne 1 ]; then
    echo "Expected exactly one argument"
    exit 1
fi

if [ "$1" = "GDB" ];
then
  timeout 40m \
    gdb -q  -ex "set print inferior-events off" -ex "set confirm off" -ex "set print thread-events off" -ex run -ex bt -ex quit --args \
    ./unit_tests_dbms --gtest_output='json:test_output/test_result.json' \
    | tee test_output/test_result.txt
elif [ "$1" = "NO_GDB" ];
then
  timeout 40m \
    ./unit_tests_dbms --gtest_output='json:test_output/test_result.json' \
    | tee test_output/test_result.txt
else
    echo "Unknown argument: $1"
    exit 1
fi
