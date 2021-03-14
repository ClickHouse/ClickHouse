#!/bin/bash

set -x

service zookeeper start && sleep 7 && /usr/share/zookeeper/bin/zkCli.sh -server localhost:2181 -create create /clickhouse_test '';
gdb -q  -ex 'set print inferior-events off' -ex 'set confirm off' -ex 'set print thread-events off' -ex run -ex bt -ex quit --args ./unit_tests_dbms | tee test_output/test_result.txt
./process_unit_tests_result.py  || echo -e "failure\tCannot parse results" > /test_output/check_status.tsv
