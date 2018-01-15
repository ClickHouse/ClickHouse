#!/bin/bash

set -o errexit
set -o pipefail

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ROOTDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && cd ../.. && pwd)
DATADIR=${DATADIR:=/tmp/clickhouse}
LOGDIR=${LOGDIR:=$DATADIR/log}

rm -rf $DATADIR

mkdir -p $LOGDIR

# Start a local clickhouse server which will be used to run tests
#PATH=$PATH:$ROOTDIR/build${BUILD_TYPE}/dbms/src/Server \
  $ROOTDIR/build${BUILD_TYPE}/dbms/src/Server/clickhouse-server --config-file=$CURDIR/server-test.xml > $LOGDIR/stdout 2>&1 &
CH_PID=$!
sleep 3

# Define needed stuff to kill test clickhouse server after tests completion
function finish {
  kill $CH_PID || true
  wait
  tail -n 50 $LOGDIR/stdout
  rm -rf $DATADIR
}
trap finish EXIT

# Do tests
export CLICKHOUSE_CONFIG=${CLICKHOUSE_CONFIG:=$CURDIR/server-test.xml}
#cd $CURDIR
if [ -n "$*" ]; then
    $*
else
    $ROOTDIR/build${BUILD_TYPE}/dbms/src/Server/clickhouse-client --config $CURDIR/client-test.xml -q 'SELECT * from system.build_options;'
    PATH=$PATH:$ROOTDIR/build${BUILD_TYPE}/dbms/src/Server \
      $CURDIR/clickhouse-test --binary $ROOTDIR/build${BUILD_TYPE}/dbms/src/Server/clickhouse --clientconfig $CURDIR/client-test.xml --tmp $DATADIR/tmp --queries $CURDIR/queries $TEST_OPT
fi
