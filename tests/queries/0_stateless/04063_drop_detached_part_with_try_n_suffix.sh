#!/usr/bin/env bash

# Regression test for parsing detached part names with the _tryN suffix.
# The bug caused BAD_DATA_PART_NAME when trying to drop such parts.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TABLE="t_04063_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (n UInt64)
    ENGINE = MergeTree ORDER BY n
"

${CLICKHOUSE_CLIENT} --query "INSERT INTO ${TABLE} VALUES (1), (42)"

# Detach the part
${CLICKHOUSE_CLIENT} --query "ALTER TABLE ${TABLE} DETACH PART 'all_0_0_0'"

# Get the path to the detached directory (parent of the part directory)
PART_PATH=$(${CLICKHOUSE_CLIENT} --query "
    SELECT path FROM system.detached_parts
    WHERE table = '${TABLE}' AND database = '${CLICKHOUSE_DATABASE}'
    LIMIT 1
")
DETACHED_DIR=$(dirname "${PART_PATH}")

# Create parts with _try1 suffix by copying the detached part
for prefix in "" "covered-by-broken_"; do
    cp -r "${DETACHED_DIR}/all_0_0_0" "${DETACHED_DIR}/${prefix}all_0_0_0_try1"
done

# List detached parts - should see all three
${CLICKHOUSE_CLIENT} --query "
    SELECT name FROM system.detached_parts
    WHERE table = '${TABLE}' AND database = '${CLICKHOUSE_DATABASE}'
    ORDER BY name
"

# Drop the detached parts with _tryN suffix - this used to fail with BAD_DATA_PART_NAME
${CLICKHOUSE_CLIENT} --query "
    ALTER TABLE ${TABLE} DROP DETACHED PART 'covered-by-broken_all_0_0_0_try1'
    SETTINGS allow_drop_detached = 1
"

${CLICKHOUSE_CLIENT} --query "
    ALTER TABLE ${TABLE} DROP DETACHED PART 'all_0_0_0_try1'
    SETTINGS allow_drop_detached = 1
"

# Only the original detached part should remain
${CLICKHOUSE_CLIENT} --query "
    SELECT name FROM system.detached_parts
    WHERE table = '${TABLE}' AND database = '${CLICKHOUSE_DATABASE}'
    ORDER BY name
"

# Re-attach and verify data
${CLICKHOUSE_CLIENT} --query "ALTER TABLE ${TABLE} ATTACH PART 'all_0_0_0'"
${CLICKHOUSE_CLIENT} --query "SELECT n FROM ${TABLE} ORDER BY n"

${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TABLE}"
