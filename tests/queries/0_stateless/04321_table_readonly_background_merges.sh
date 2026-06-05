#!/usr/bin/env bash
# Tags: long
# ^ long: waits for the background merge pool to make progress within a bounded time window.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A read-only table (the `table_readonly` MergeTree setting) must not waste background CPU
# on regular merges, but it must still drop expired data via TTL so that disk is reclaimed.

${CLICKHOUSE_CLIENT} -m -q "
DROP TABLE IF EXISTS t_readonly;
DROP TABLE IF EXISTS t_writable;

CREATE TABLE t_readonly (x UInt64) ENGINE = MergeTree ORDER BY x;
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

# Now check that TTL still reclaims disk on a read-only table.
${CLICKHOUSE_CLIENT} -m -q "
DROP TABLE IF EXISTS t_readonly_ttl;

CREATE TABLE t_readonly_ttl (d Date, x UInt64)
ENGINE = MergeTree ORDER BY x
TTL d + INTERVAL 1 DAY
SETTINGS ttl_only_drop_parts = 1, merge_with_ttl_timeout = 0;

-- One fully expired part and one fresh part.
INSERT INTO t_readonly_ttl VALUES (today() - 10, 1);
INSERT INTO t_readonly_ttl VALUES (today(), 2);

ALTER TABLE t_readonly_ttl MODIFY SETTING table_readonly = 1;
"

# Wait until the expired part is dropped by a background TTL merge.
dropped=0
for _ in $(seq 1 120); do
    count=$(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_readonly_ttl")
    if [[ "$count" -eq 1 ]]; then
        dropped=1
        break
    fi
    sleep 0.5
done

if [[ "$dropped" -eq 1 ]]; then
    echo "TTL drop on readonly: OK"
else
    echo "TTL drop on readonly: FAIL (remaining rows: $(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_readonly_ttl"))"
fi

# The surviving row must be the fresh one.
echo "surviving row:"
${CLICKHOUSE_CLIENT} -q "SELECT x FROM t_readonly_ttl ORDER BY x"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_readonly_ttl"
