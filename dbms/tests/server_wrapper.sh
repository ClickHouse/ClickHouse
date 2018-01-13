#!/bin/bash

set -o errexit
set -o pipefail

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ROOTDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && cd ../.. && pwd)

# Start a local clickhouse server which will be used to run tests
PATH=$PATH:$ROOTDIR/build/dbms/src/Server \
  $ROOTDIR/build/dbms/src/Server/clickhouse-server --config-file=$ROOTDIR/dbms/tests/server-test.xml 2>/dev/null &
CH_PID=$!
#sleep 3

# Define needed stuff to kill test clickhouse server after tests completion
function finish {
  kill $CH_PID
  wait
  rm -rf /tmp/clickhouse
}
trap finish EXIT

# Do tests
export CLICKHOUSE_CONFIG=${CLICKHOUSE_CONFIG:=$ROOTDIR/dbms/tests/server-test.xml}
cd $ROOTDIR/dbms/tests
PATH=$PATH:$ROOTDIR/build/dbms/src/Server \
  ./clickhouse-test -c "$ROOTDIR/build/dbms/src/Server/clickhouse-client --config $ROOTDIR/dbms/tests/clickhouse-client.xml"
