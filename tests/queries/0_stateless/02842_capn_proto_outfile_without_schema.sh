#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


$CLICKHOUSE_LOCAL -q "select * from numbers(10) into outfile '$CLICKHOUSE_TEST_UNIQUE_NAME.capnp'" 2>&1 | grep "The format CapnProto requires a schema" -c
rm $CLICKHOUSE_TEST_UNIQUE_NAME.capnp

