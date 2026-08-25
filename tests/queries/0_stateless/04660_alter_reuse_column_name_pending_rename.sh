#!/usr/bin/env bash
# Tags: no-replicated-database, no-shared-merge-tree
# Regression test for https://github.com/ClickHouse/ClickHouse/issues/109957

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./mergetree_mutations.lib
. "$CUR_DIR"/mergetree_mutations.lib

${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS t_reuse_name_rename;

    CREATE TABLE t_reuse_name_rename (id UInt64, a String)
    ENGINE = MergeTree ORDER BY id;

    INSERT INTO t_reuse_name_rename VALUES (1, 'hello'), (2, 'world');

    SYSTEM STOP MERGES t_reuse_name_rename;
"

${CLICKHOUSE_CLIENT} --alter_sync=0 --mutations_sync=0 -q "ALTER TABLE t_reuse_name_rename RENAME COLUMN a TO b"

(
    ${CLICKHOUSE_CLIENT} --alter_sync=0 --mutations_sync=0 -q "ALTER TABLE t_reuse_name_rename DROP COLUMN b"
    ${CLICKHOUSE_CLIENT} -q "ALTER TABLE t_reuse_name_rename ADD COLUMN b UInt64"
) &

sleep 2

for _ in {1..100}
do
    count=$(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.processes WHERE current_database = currentDatabase() AND (query ILIKE '%DROP COLUMN%' OR query ILIKE '%ADD COLUMN%') AND query NOT ILIKE '%system.processes%'")
    [[ $count -ge 1 ]] && break
    sleep 0.3
done

${CLICKHOUSE_CLIENT} -q "SYSTEM START MERGES t_reuse_name_rename"

wait

wait_for_all_mutations "t_reuse_name_rename"

${CLICKHOUSE_CLIENT} -q "SELECT id, b FROM t_reuse_name_rename ORDER BY id"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_reuse_name_rename' AND NOT is_done"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_reuse_name_rename"
