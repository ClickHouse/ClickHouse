#!/bin/bash

set -x

timeout 40m gdb -q  -ex 'set print inferior-events off' -ex 'set confirm off' -ex 'set print thread-events off' -ex run -ex bt -ex quit --args ./unit_tests_dbms --gtest_output='json:test_output/test_result.json' | tee test_output/test_result.txt
