#!/usr/bin/env bash
# Tags: no-parallel, no-replicated-database, no-shared-catalog, no-fasttest
# Tag no-parallel -- edits the table's on-disk metadata and re-attaches
# Tag no-replicated-database, no-shared-catalog -- editing on-disk metadata breaks the Replicated database digest
# Tag no-fasttest -- statistics require a full build

# Reproducer for the "Type mismatch when building statistics" LOGICAL_ERROR (issue #109611).
# A stat-bearing column is walked (by the AST fuzzer, in the wild) to an ALIAS column of a
# different type while an old part still stores it physically with the original-type statistics
# file. A later MATERIALIZE COLUMN mutation writes the column at the new type but feeds the
# stale original-type statistics collector, which used to abort the mutation.
# The fuzzer reaches the ALIAS state through a long MODIFY chain that is not directly issuable
# (a physical stat-bearing column cannot be turned into ALIAS synchronously), so the wedged
# state is reconstructed here by editing the table metadata and re-attaching, mirroring the
# on-disk snapshots reported on the issue.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_stats_alias SYNC"

${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE t_stats_alias (id Int64, payload Int64, mtrl Int64 STATISTICS(uniq, minmax))
    ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 1, index_granularity = 8192"

${CLICKHOUSE_CLIENT} -q "INSERT INTO t_stats_alias SELECT number, number, number FROM numbers(500)"
# Write the statistics file for mtrl at its original type (Int64).
${CLICKHOUSE_CLIENT} -q "ALTER TABLE t_stats_alias MATERIALIZE STATISTICS mtrl SETTINGS mutations_sync = 2"

# system.tables.metadata_path is relative to the database disk root, so prepend the disk path.
metadata_path=$(${CLICKHOUSE_CLIENT} -q "SELECT metadata_path FROM system.tables WHERE table = 't_stats_alias' AND database = currentDatabase()")

${CLICKHOUSE_CLIENT} -q "DETACH TABLE t_stats_alias"

# Rewrite the column definition to what the fuzzer eventually reaches: mtrl as an ALIAS column of
# a different type. The part on disk is untouched, so it still carries the physical Int64 column
# and its Int64 statistics file.
sed_expr="s/\`mtrl\` Int64 STATISTICS(uniq, minmax)/\`mtrl\` Int16 ALIAS 1/"
data_path=$(${CLICKHOUSE_CLIENT} -q "SELECT path FROM system.disks WHERE name = 'default'")
if [ -e "$data_path$metadata_path" ]; then
    sed -i -e "$sed_expr" "$data_path$metadata_path"
else
    # The database metadata lives on a remote disk (the "db disk" parametrization).
    config="${CURDIR}/04501_statistics_type_mismatch_after_alias_modify.xml"
    metadata=$(clickhouse-disks -C "$config" --disk "disk_db_remote" --save-logs --query "read $metadata_path")
    metadata_updated=$(echo "$metadata" | sed -e "$sed_expr")
    echo "$metadata_updated" | clickhouse-disks -C "$config" --disk "disk_db_remote" --save-logs --query "write --path-to $metadata_path"
    ${CLICKHOUSE_CLIENT} -q "SYSTEM DROP DISK METADATA CACHE 'disk_db_remote'"
fi

${CLICKHOUSE_CLIENT} -q "ATTACH TABLE t_stats_alias"

# Before the fix this MATERIALIZE COLUMN aborted the server with
# 'Type mismatch when building statistics for column mtrl: statistics expect type Int64 but block has type Int16'.
${CLICKHOUSE_CLIENT} -q "ALTER TABLE t_stats_alias MATERIALIZE COLUMN mtrl SETTINGS mutations_sync = 2"

${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_stats_alias"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_stats_alias SYNC"
