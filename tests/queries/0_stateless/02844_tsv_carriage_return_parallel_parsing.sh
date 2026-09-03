#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -q "select 'Hello\rWorld' from numbers(1000000) format TSVRaw" > $CLICKHOUSE_TEST_UNIQUE_NAME.tsv
$CLICKHOUSE_LOCAL -q "select count() from file('$CLICKHOUSE_TEST_UNIQUE_NAME.tsv')"
rm $CLICKHOUSE_TEST_UNIQUE_NAME.tsv

