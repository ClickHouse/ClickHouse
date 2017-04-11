#!/bin/bash

set -o errexit
set -o pipefail

ln -sf clickhouse build/dbms/src/Server/clickhouse-server
ln -sf clickhouse build/dbms/src/Server/clickhouse-client
ln -sf clickhouse build/dbms/src/Server/clickhouse-local
# Start a local clickhouse server which will be used to run tests
PWD=$(pwd)
echo ${PWD}
PATH=$PATH:./build/dbms/src/Server \
  ./build/dbms/src/Server/clickhouse-server --config-file=./debian/clickhouse-server-config-tests.xml 2>/dev/null &
CH_PID=$!

# Define needed stuff to kill test clickhouse server after tests completion
function finish {
  kill $CH_PID
  wait
  rm -rf /tmp/clickhouse
  rm -f build/dbms/src/Server/clickhouse-local
  rm -f build/dbms/src/Server/clickhouse-client
  rm -f build/dbms/src/Server/clickhouse-server
}
trap finish EXIT

# Do tests
cd dbms/tests
PATH=$PATH:../../build/dbms/src/Server \
  ./clickhouse-test -c ../../build/dbms/src/Server/clickhouse-client
