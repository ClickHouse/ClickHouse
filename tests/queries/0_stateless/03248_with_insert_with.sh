#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q 'DROP TABLE IF EXISTS x'
$CLICKHOUSE_CLIENT -q 'CREATE TABLE x ENGINE = Log AS SELECT * FROM numbers(0)'
$CLICKHOUSE_CLIENT -q 'WITH y AS ( SELECT * FROM numbers(10) ) INSERT INTO x WITH y2 AS ( SELECT * FROM numbers(10) ) SELECT * FROM y;' |& grep --max-count 1 -F --only-matching "Only one WITH should be presented, either before INSERT or SELECT"
$CLICKHOUSE_CLIENT -q 'DROP TABLE x'