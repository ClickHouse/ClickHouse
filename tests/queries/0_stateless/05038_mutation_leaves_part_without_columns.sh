#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: uses the server-global failpoint mt_select_parts_to_mutate_no_free_threads

# A mutation can leave a part with no columns: it drops every column the part was written with, while
# the columns the table has now were added afterwards and were never materialised in it. Building the
# preferred file order for such a part used to read its first column, and there is none.
#
# The call site that does that is compiled only in the cloud build
# (`MutatePlainMergeTreeTask.cpp`, `#if CLICKHOUSE_CLOUD`), so on the open-source build this test only
# asserts that the mutation completes; it is the sync run that exercises the repaired path.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The failpoint is server-global: release it even if a query below fails, or every later test on this
# server would run with mutations disabled.
cleanup() {
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads" 2>/dev/null
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_no_columns_left" 2>/dev/null
}
trap cleanup EXIT

# Both DROPs pass the "cannot drop all columns" check: it tests the table's column set, which keeps
# `c`. What empties is the set of columns the parts hold.
$CLICKHOUSE_CLIENT -mq "
DROP TABLE IF EXISTS t_no_columns_left;

CREATE TABLE t_no_columns_left (a UInt8, b UInt8)
ENGINE = MergeTree()
ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_no_columns_left VALUES (1, 10);
INSERT INTO t_no_columns_left VALUES (2, 20);

ALTER TABLE t_no_columns_left ADD COLUMN c UInt64 DEFAULT 42;

SYSTEM ENABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads;

SET alter_sync = 0;
ALTER TABLE t_no_columns_left DROP COLUMN a;
ALTER TABLE t_no_columns_left DROP COLUMN b;

SYSTEM DISABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads;
"

# Wait for both mutations to materialise the column-less parts.
for _ in {1..100}; do
    pending=$($CLICKHOUSE_CLIENT -q "
        SELECT count() FROM system.mutations
        WHERE table = 't_no_columns_left' AND database = currentDatabase() AND NOT is_done")
    [ "$pending" = "0" ] && break
    sleep 0.3
done

echo -n 'mutations done: '
$CLICKHOUSE_CLIENT -q "
    SELECT countIf(is_done) = count() AND count() = 2 FROM system.mutations
    WHERE table = 't_no_columns_left' AND database = currentDatabase()"

echo -n 'parts and rows: '
$CLICKHOUSE_CLIENT -q "
    SELECT count(), sum(rows) FROM system.parts
    WHERE table = 't_no_columns_left' AND database = currentDatabase() AND active"

echo -n 'rows readable: '
$CLICKHOUSE_CLIENT -q "SELECT count() FROM t_no_columns_left"
