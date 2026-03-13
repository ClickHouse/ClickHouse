#!/usr/bin/env bash
# Tags: zookeeper

# Test backward compatibility for implicit minmax indices in ZK metadata.
# Older versions (<=25.10) stored implicit auto_minmax_index_* indices in
# the ZK metadata via toString(). Newer versions use explicitToString() which
# excludes them. IndicesDescription::parse() must detect implicit indices by
# name prefix so that checkEquals() backward-compatibility fallback works.
# Without the fix, SYSTEM RESTART REPLICA would fail with METADATA_MISMATCH.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_implicit_minmax_compat_${CLICKHOUSE_DATABASE}"
ZK_PATH="/clickhouse/tables/${CLICKHOUSE_DATABASE}/${TABLE}"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE}
    (
        id UInt64,
        value Float64,
        name String,
        INDEX user_idx value TYPE minmax GRANULARITY 1
    )
    ENGINE = ReplicatedMergeTree('${ZK_PATH}', '1')
    ORDER BY id
    SETTINGS add_minmax_index_for_numeric_columns = 1
"

${CLICKHOUSE_CLIENT} --query "INSERT INTO ${TABLE} VALUES (1, 1.5, 'a'), (2, 2.5, 'b')"

# Read current ZK metadata
ZK_METADATA=$(${CLICKHOUSE_CLIENT} --query "
    SELECT value FROM system.zookeeper
    WHERE path = '${ZK_PATH}' AND name = 'metadata'
")

# The current metadata has indices line with only explicit indices (new format).
# Modify it to include implicit indices, simulating old (25.10) format where
# toString() included auto_minmax_index_* entries.
# We append implicit indices to the existing indices line.
ZK_METADATA_OLD_FORMAT=$(echo "${ZK_METADATA}" | sed "s|^indices: \(.*\)|indices: \1, INDEX auto_minmax_index_id id TYPE minmax GRANULARITY 1, INDEX auto_minmax_index_value value TYPE minmax GRANULARITY 1|")

# Write the old-format metadata back to ZK
${CLICKHOUSE_KEEPER_CLIENT} -q "set '${ZK_PATH}/metadata' '${ZK_METADATA_OLD_FORMAT}'"

# Restart replica to trigger checkTableStructure -> checkEquals.
# Without the fix, this fails with METADATA_MISMATCH on "skip indexes".
${CLICKHOUSE_CLIENT} --query "SYSTEM RESTART REPLICA ${TABLE}"

# Verify table is accessible
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${TABLE}"

${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TABLE}"
