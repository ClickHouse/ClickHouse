#!/usr/bin/env bash
# Tags: long
# ^ long: waits for the background merge pool to make progress within a bounded time window.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A read-only table (the `table_readonly` MergeTree setting) performs no modifications on disk and
# wastes no background CPU: neither regular merges nor TTL drop/delete merges run on it.

${CLICKHOUSE_CLIENT} -m -q "
DROP TABLE IF EXISTS t_readonly;
DROP TABLE IF EXISTS t_writable;

-- min_age_to_force_merge_seconds covers the forced whole-partition merge fallback
-- (getBestPartitionToOptimizeEntire): it must not merge a read-only table either.
CREATE TABLE t_readonly (x UInt64) ENGINE = MergeTree ORDER BY x
SETTINGS min_age_to_force_merge_seconds = 1, min_age_to_force_merge_on_partition_only = 1;
CREATE TABLE t_writable (x UInt64) ENGINE = MergeTree ORDER BY x;

-- Stop merges so the parts are not merged before the table is marked read-only.
SYSTEM STOP MERGES t_readonly;
SYSTEM STOP MERGES t_writable;
"

# Create 10 separate parts in each table.
for i in $(seq 1 10); do
    ${CLICKHOUSE_CLIENT} -q "INSERT INTO t_readonly VALUES (${i})"
    ${CLICKHOUSE_CLIENT} -q "INSERT INTO t_writable VALUES (${i})"
done

${CLICKHOUSE_CLIENT} -m -q "
-- Mark the first table as read-only, then allow merges again on both.
ALTER TABLE t_readonly MODIFY SETTING table_readonly = 1;
SYSTEM START MERGES t_readonly;
SYSTEM START MERGES t_writable;
"

# Wait until the writable table gets its parts merged in the background.
# This proves the background merge pool is actively making progress right now.
merged=0
for _ in $(seq 1 120); do
    count=$(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_writable' AND active")
    if [[ "$count" -lt 10 ]]; then
        merged=1
        break
    fi
    sleep 0.5
done

if [[ "$merged" -ne 1 ]]; then
    echo "FAIL: writable control table was not merged in the background"
fi

# The read-only table must still have all 10 parts: no regular merge should have happened.
echo "readonly active parts:"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_readonly' AND active"

${CLICKHOUSE_CLIENT} -m -q "
DROP TABLE t_readonly;
DROP TABLE t_writable;
"

# Now check that a read-only table does NOT drop expired data via TTL: it must not modify data on
# disk at all. A writable control table with the same TTL proves the background TTL merge path is
# actively making progress; the read-only table must keep its expired part untouched.
${CLICKHOUSE_CLIENT} -m -q "
DROP TABLE IF EXISTS t_readonly_ttl;
DROP TABLE IF EXISTS t_writable_ttl;

-- The TTL margin must be much larger than one day: the test runs with a randomized
-- session_timezone, so today() may differ from the server date by a day in either direction.
CREATE TABLE t_readonly_ttl (d Date, x UInt64)
ENGINE = MergeTree ORDER BY x
TTL d + INTERVAL 1 MONTH
SETTINGS ttl_only_drop_parts = 1, merge_with_ttl_timeout = 0;
CREATE TABLE t_writable_ttl (d Date, x UInt64)
ENGINE = MergeTree ORDER BY x
TTL d + INTERVAL 1 MONTH
SETTINGS ttl_only_drop_parts = 1, merge_with_ttl_timeout = 0;

-- Stop merges before inserting the expired part, so it cannot be dropped by a TTL merge
-- before the read-only table is marked.
SYSTEM STOP MERGES t_readonly_ttl;
SYSTEM STOP MERGES t_writable_ttl;

-- One fully expired part and one fresh part in each table.
INSERT INTO t_readonly_ttl VALUES (today() - 100, 1);
INSERT INTO t_readonly_ttl VALUES (today(), 2);
INSERT INTO t_writable_ttl VALUES (today() - 100, 1);
INSERT INTO t_writable_ttl VALUES (today(), 2);

ALTER TABLE t_readonly_ttl MODIFY SETTING table_readonly = 1;
SYSTEM START MERGES t_readonly_ttl;
SYSTEM START MERGES t_writable_ttl;
"

# Wait until the writable control table drops its expired part via a background TTL merge.
# This proves the background TTL merge path is actively making progress right now.
dropped=0
for _ in $(seq 1 120); do
    count=$(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_writable_ttl")
    if [[ "$count" -eq 1 ]]; then
        dropped=1
        break
    fi
    sleep 0.5
done

if [[ "$dropped" -ne 1 ]]; then
    echo "FAIL: writable control table TTL did not run in the background"
fi

# The read-only table must still have BOTH rows: no TTL merge should have happened.
echo "readonly ttl rows:"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_readonly_ttl"

${CLICKHOUSE_CLIENT} -m -q "
DROP TABLE t_readonly_ttl;
DROP TABLE t_writable_ttl;
"
