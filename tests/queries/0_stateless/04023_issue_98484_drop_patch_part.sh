#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# Tag no-fasttest: requires lightweight_delete_mode setting

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Test for issue #98484: DROP PART on patch part should not crash server.
# The bug was that getPatchPartMetadata() built a partition key expression
# referencing _part column, but the ColumnsDescription passed from
# createEmptyPart() only contained data columns, causing UNKNOWN_IDENTIFIER
# inside a NOEXCEPT_SCOPE which triggered std::terminate().

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE t_98484 (c0 Int32, c1 String, c2 Int8)
    ENGINE = MergeTree() ORDER BY tuple()
    SETTINGS enable_block_offset_column = 1, enable_block_number_column = 1
"

${CLICKHOUSE_CLIENT} --query "INSERT INTO t_98484 VALUES (1, 'hello', 10)"
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_98484 VALUES (2, 'world', 20)"
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_98484 VALUES (3, 'test', 30)"

# Create patch parts via lightweight delete
${CLICKHOUSE_CLIENT} --query "SET lightweight_delete_mode = 'lightweight_update_force'; DELETE FROM t_98484 WHERE c0 = 1"

# Wait for mutations to complete
for _ in $(seq 1 30); do
    result=$(${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_98484' AND name LIKE 'patch-%' AND active = 1")
    if [ "$result" -ge 1 ]; then
        break
    fi
    sleep 0.5
done

# Add column to change columns description (original trigger condition)
${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_98484 ADD COLUMN c9 Nullable(Bool)"

# Get the first active patch part name
PATCH_PART=$(${CLICKHOUSE_CLIENT} --query "
    SELECT name FROM system.parts
    WHERE database = currentDatabase() AND table = 't_98484'
        AND name LIKE 'patch-%' AND active = 1
    ORDER BY name LIMIT 1
")

if [ -z "$PATCH_PART" ]; then
    echo "FAIL: No patch parts found"
    exit 1
fi

# DROP PART on the patch part - this should not crash the server
${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_98484 DROP PART '$PATCH_PART'" 2>&1

# Verify server is still alive
${CLICKHOUSE_CLIENT} --query "SELECT 1" > /dev/null 2>&1
if [ $? -ne 0 ]; then
    echo "FAIL: Server crashed"
    exit 1
fi

echo "OK"

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_98484"
