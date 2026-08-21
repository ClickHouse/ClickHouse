#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# ^ the Vortex format is not included in the fast test and MSan builds

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `Vortex` is left out of the all-formats matrix in `02187_async_inserts_all_formats` because it is
# not in every build, so its async insert path is covered here.

DATA_FILE=$CLICKHOUSE_TMP/test_$CLICKHOUSE_TEST_UNIQUE_NAME.vortex

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_vortex_async_insert;
    CREATE TABLE t_vortex_async_insert (id UInt64, s String, arr Array(UInt64)) ENGINE = Memory;
"

$CLICKHOUSE_LOCAL -q "
    SELECT number AS id, toString(number) AS s, range(number % 3) AS arr FROM numbers(5)
    INTO OUTFILE '$DATA_FILE' TRUNCATE FORMAT Vortex
"

for _ in 1 2
do
    $CLICKHOUSE_CLIENT --async_insert 1 --wait_for_async_insert 1 \
        -q "INSERT INTO t_vortex_async_insert FORMAT Vortex" < "$DATA_FILE"
done

echo "Rows:"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM t_vortex_async_insert"
echo "Data:"
$CLICKHOUSE_CLIENT -q "SELECT * FROM t_vortex_async_insert ORDER BY id, s FORMAT TSV"

echo "Truncated file:"
head -c 100 "$DATA_FILE" > "$DATA_FILE.truncated"
$CLICKHOUSE_CLIENT --async_insert 1 --wait_for_async_insert 1 \
    -q "INSERT INTO t_vortex_async_insert FORMAT Vortex" < "$DATA_FILE.truncated" 2>&1 \
    | grep -q -F 'DB::Exception' && echo "failed as expected"

echo "Rows after the failed insert:"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM t_vortex_async_insert"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_vortex_async_insert"
rm -f "$DATA_FILE" "$DATA_FILE.truncated"
