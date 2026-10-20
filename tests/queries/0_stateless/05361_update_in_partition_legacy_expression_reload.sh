#!/usr/bin/env bash
# Tags: no-shared-merge-tree, no-object-storage
# Tag no-shared-merge-tree: rewrites the `mutation_*.txt` file of a `MergeTree` table.
# Tag no-object-storage: the file is edited on the local disk.

# A mutation entry written by an older version keeps the partition expression of `IN PARTITION`
# instead of the `ID '...'` it resolves to. Such an expression is arbitrary SQL and may refer to tables
# that are not available while the table is being loaded, so it must not be evaluated then: `ATTACH`
# used to fail with `UNKNOWN_TABLE`. The command is not applied on the fly, and its partitions are
# evaluated when the mutation materializes.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS t_legacy_partition_expression;
    DROP TABLE IF EXISTS t_legacy_partition_expression_aux;
    CREATE TABLE t_legacy_partition_expression_aux (p UInt8) ENGINE = MergeTree ORDER BY p;
    INSERT INTO t_legacy_partition_expression_aux VALUES (1);
    CREATE TABLE t_legacy_partition_expression (p UInt8, id UInt64, v UInt64)
        ENGINE = MergeTree PARTITION BY p ORDER BY id
        SETTINGS storage_policy = 'default', max_postpone_time_for_failed_mutations_ms = 0;
    INSERT INTO t_legacy_partition_expression VALUES (1, 1, 10), (2, 2, 20);
    SYSTEM STOP MERGES t_legacy_partition_expression;
    ALTER TABLE t_legacy_partition_expression UPDATE v = v + 100 IN PARTITION ID '1' WHERE 1 SETTINGS alter_sync = 0;
"

echo 'stored command'
${CLICKHOUSE_CLIENT} -q "SELECT command FROM system.mutations WHERE database = currentDatabase() AND table = 't_legacy_partition_expression'"

data_path=$(${CLICKHOUSE_CLIENT} -q "SELECT data_paths[1] FROM system.tables WHERE database = currentDatabase() AND table = 't_legacy_partition_expression'")

${CLICKHOUSE_CLIENT} -q "DETACH TABLE t_legacy_partition_expression"

# Restore the entry as an older version wrote it, with an expression over a table that is detached now.
sed -i "s/IN PARTITION ID \\\\'1\\\\'/IN PARTITION tuple((SELECT min(p) FROM ${CLICKHOUSE_DATABASE}.t_legacy_partition_expression_aux))/" "${data_path}"mutation_*.txt

${CLICKHOUSE_CLIENT} -q "DETACH TABLE t_legacy_partition_expression_aux"
${CLICKHOUSE_CLIENT} -q "ATTACH TABLE t_legacy_partition_expression"
${CLICKHOUSE_CLIENT} -q "SYSTEM STOP MERGES t_legacy_partition_expression"

echo 'stored command after the table is reloaded'
${CLICKHOUSE_CLIENT} -q "SELECT command FROM system.mutations WHERE database = currentDatabase() AND table = 't_legacy_partition_expression'" \
    | sed "s/${CLICKHOUSE_DATABASE}\./db./"

echo 'pending update, read on the fly'
${CLICKHOUSE_CLIENT} -q "SELECT p, id, v FROM t_legacy_partition_expression ORDER BY id SETTINGS apply_mutations_on_fly = 1"

${CLICKHOUSE_CLIENT} -q "
    ATTACH TABLE t_legacy_partition_expression_aux;
    SYSTEM START MERGES t_legacy_partition_expression;
    ALTER TABLE t_legacy_partition_expression UPDATE v = v WHERE 0 SETTINGS mutations_sync = 2;
"

echo 'after it materialized'
${CLICKHOUSE_CLIENT} -q "SELECT p, id, v FROM t_legacy_partition_expression ORDER BY id"

${CLICKHOUSE_CLIENT} -q "
    DROP TABLE t_legacy_partition_expression;
    DROP TABLE t_legacy_partition_expression_aux;
"
