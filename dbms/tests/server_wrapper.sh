#!/usr/bin/env bash

set -o errexit
set -o pipefail

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && cd ../.. && pwd)
DATA_DIR=${DATA_DIR:=/tmp/clickhouse}
LOG_DIR=${LOG_DIR:=$DATA_DIR/log}
BUILD_DIR=${BUILD_DIR:="$ROOT_DIR/build${BUILD_TYPE}"}

rm -rf $DATA_DIR

mkdir -p $LOG_DIR

# Start a local clickhouse server which will be used to run tests
#PATH=$PATH:$BUILD_DIR/dbms/src/Server \
  $BUILD_DIR/dbms/src/Server/clickhouse-server --config-file=$CUR_DIR/server-test.xml > $LOG_DIR/stdout 2>&1 &
CH_PID=$!
sleep 3

# Define needed stuff to kill test clickhouse server after tests completion
function finish {
  kill $CH_PID || true
  wait
  tail -n 50 $LOG_DIR/stdout
  rm -rf $DATA_DIR
}
trap finish EXIT

# Do tests
export CLICKHOUSE_CONFIG=${CLICKHOUSE_CONFIG:=$CUR_DIR/server-test.xml}
#cd $CUR_DIR
if [ -n "$*" ]; then
    $*
else
    $BUILD_DIR/dbms/src/Server/clickhouse-client --config $CUR_DIR/client-test.xml -q 'SELECT * from system.build_options;'
    PATH=$PATH:$BUILD_DIR/dbms/src/Server \
      $CUR_DIR/clickhouse-test --binary $BUILD_DIR/dbms/src/Server/clickhouse --configclient $CUR_DIR/client-test.xml --configserver $CUR_DIR/server-test.xml --tmp $DATA_DIR/tmp --queries $CUR_DIR/queries $TEST_OPT
fi
