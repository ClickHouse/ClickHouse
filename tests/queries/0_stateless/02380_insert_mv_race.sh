#!/usr/bin/env bash
# Tags: long, race

# Regression test for INSERT into table with MV attached,
# to avoid possible errors if some table will disappears,
# in case of multiple streams was used (i.e. max_insert_threads>1)

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -nm -q "
    DROP TABLE IF EXISTS null;
    CREATE TABLE null (key Int) ENGINE = Null;
    DROP TABLE IF EXISTS mv;
    CREATE MATERIALIZED VIEW mv ENGINE = Null() AS SELECT * FROM null;
"

$CLICKHOUSE_CLIENT -q "INSERT INTO null SELECT * FROM numbers_mt(1000) settings max_threads=1000, max_insert_threads=1000, max_block_size=1" |& {
    # To avoid handling stacktrace here, get only first line (-m1)
    # this should be OK, since you cannot have multiple exceptions from the client anyway.
    grep -m1 -F 'DB::Exception:' | grep -F -v -e 'UNKNOWN_TABLE'
} &
sleep 0.05
$CLICKHOUSE_CLIENT -q "DETACH TABLE mv"
wait
