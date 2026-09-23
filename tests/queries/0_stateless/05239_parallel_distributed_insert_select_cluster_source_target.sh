#!/usr/bin/env bash
# Tags: no-fasttest, zookeeper
#       no-fasttest: a cluster table function over a cluster of three nodes, and `ReplicatedMergeTree`.
#
# With `parallel_distributed_insert_select = 2` and a cluster table function as the source, the whole
# INSERT is forwarded to every node of that cluster and each node runs it over its own slice of the
# read. That adds up to one logical INSERT only where every node's write becomes visible on all of
# them: each node also pushes its slice through the target's dependent materialized views, so a view
# target that does not replicate keeps a different subset of the rows on each node, and an `Alias`
# reports the engine of the table it points at while owning views of its own that the check on the
# forwarded-to table does not see. Both shapes fall back to the initiator. The first case below is the
# control that the rig forwards the INSERT at all.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

mkdir -p "${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
DATA_FILE="${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/data.csv"
${CLICKHOUSE_CLIENT} -q "SELECT number FROM numbers(30000) FORMAT CSV" > "${DATA_FILE}"

SRC="fileCluster('test_cluster_one_shard_three_replicas_localhost', '${CLICKHOUSE_TEST_UNIQUE_NAME}/data.csv', 'CSV', 'k UInt64')"

${CLICKHOUSE_CLIENT} -q "
CREATE TABLE dst_plain (k UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/05239_plain', 'r1') ORDER BY k;

CREATE TABLE dst_view (k UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/05239_view', 'r1') ORDER BY k;
CREATE MATERIALIZED VIEW mv ENGINE = MergeTree ORDER BY k AS SELECT k FROM dst_view;

CREATE TABLE dst_alias (k UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/05239_alias', 'r1') ORDER BY k;
CREATE TABLE al ENGINE = Alias(currentDatabase(), 'dst_alias');
CREATE TABLE al_view_dest (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE MATERIALIZED VIEW al_view TO al_view_dest AS SELECT k FROM al;

CREATE TABLE dst_bare (k UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/05239_bare', 'r1') ORDER BY k;
CREATE TABLE al_bare ENGINE = Alias(currentDatabase(), 'dst_bare');
"

# Every INSERT runs first; the query log is flushed once, below, before the assertions.
for target in dst_plain dst_view al al_bare; do
    ${CLICKHOUSE_CLIENT} -q "
    INSERT INTO ${target} SELECT k FROM ${SRC}
    SETTINGS parallel_distributed_insert_select = 2, enable_analyzer = 1, log_comment = '05239_${target}'"
done

${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS query_log"

# The forwarded INSERT is logged once per node, so more than one execution means the write fan-out was
# used. The forwarded query text names the target with its database, so the initial and the forwarded
# rows are matched either by `current_database` or by the databases the query touched.
forwarded() {
    ${CLICKHOUSE_CLIENT} -q "
    SELECT '$1', count() > 1 FROM system.query_log
    WHERE type = 'QueryStart' AND query_kind = 'Insert' AND log_comment = '05239_$2'
        AND (current_database = currentDatabase() OR has(databases, currentDatabase()))
        AND event_date >= yesterday() AND event_time >= now() - 600"
}

# A fallback still reads the source on every node of its cluster, from the initiator. Without this the
# row counts would also hold if the source had silently degraded to a single local reader.
distributed_read() {
    ${CLICKHOUSE_CLIENT} -q "
    SELECT '$1', maxIf(ProfileEvents['Shards'] > 1, is_initial_query) FROM system.query_log
    WHERE type = 'QueryFinish' AND query_kind = 'Insert' AND log_comment = '05239_$2'
        AND (current_database = currentDatabase() OR has(databases, currentDatabase()))
        AND event_date >= yesterday() AND event_time >= now() - 600"
}

forwarded 'no view: distributed write' dst_plain
${CLICKHOUSE_CLIENT} -q "SELECT 'no view: rows', count() FROM dst_plain"

forwarded 'view: distributed write' dst_view
distributed_read 'view: distributed read' dst_view
${CLICKHOUSE_CLIENT} -q "SELECT 'view: rows', count() FROM dst_view"
${CLICKHOUSE_CLIENT} -q "SELECT 'view: view rows', count() FROM mv"

forwarded 'alias with a view on the alias: distributed write' al
distributed_read 'alias with a view on the alias: distributed read' al
${CLICKHOUSE_CLIENT} -q "SELECT 'alias with a view on the alias: rows', count() FROM dst_alias"
${CLICKHOUSE_CLIENT} -q "SELECT 'alias with a view on the alias: view rows', count() FROM al_view_dest"

forwarded 'alias without any view: distributed write' al_bare
distributed_read 'alias without any view: distributed read' al_bare
${CLICKHOUSE_CLIENT} -q "SELECT 'alias without any view: rows', count() FROM dst_bare"

${CLICKHOUSE_CLIENT} -q "
DROP TABLE al_bare; DROP TABLE dst_bare SYNC;
DROP TABLE al_view; DROP TABLE al_view_dest; DROP TABLE al; DROP TABLE dst_alias SYNC;
DROP TABLE mv; DROP TABLE dst_view SYNC;
DROP TABLE dst_plain SYNC;
"
rm -rf "${USER_FILES_PATH:?}/${CLICKHOUSE_TEST_UNIQUE_NAME:?}"
