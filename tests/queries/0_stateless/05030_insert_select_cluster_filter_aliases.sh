#!/usr/bin/env bash
# Tags: no-fasttest, zookeeper
# Tag no-fasttest: needs a Replicated table (the parallel distributed INSERT SELECT path requires a destination that supports replication)

# `INSERT INTO <replicated table> SELECT ... FROM <cluster table function>` with
# `parallel_distributed_insert_select` distributes the read tasks from the initiator, and the
# initiator prunes the file list with the `WHERE`/`PREWHERE` condition of the `SELECT`.
#
# The condition is analyzed on its own, out of the context of the query, so aliases introduced in
# the `WITH` clause or in the `SELECT` list have to be substituted first. They used to be left as
# is, and the analysis died with `UNKNOWN_IDENTIFIER`, which both disabled the pruning and wrote a
# stack trace into the server log for a perfectly normal query.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -u

DATA_DIR="${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}_05030"
rm -rf "${DATA_DIR}"
mkdir -p "${DATA_DIR}"

# Only `part_1.tsv` can be parsed as `x UInt32`. If the pruning does not happen, a worker also gets
# `part_2.tsv` as a read task and the query fails, so the result below proves the filter was built.
printf '1\n2\n' > "${DATA_DIR}/part_1.tsv"
printf 'not a number\n' > "${DATA_DIR}/part_2.tsv"

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS dst_05030 SYNC;
    CREATE TABLE dst_05030 (x UInt32)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/dst_05030', 'r1') ORDER BY x;
"

QUERY_ID="05030_${CLICKHOUSE_DATABASE}_with_alias"

echo "--- alias from the WITH clause ---"
$CLICKHOUSE_CLIENT --query_id "${QUERY_ID}" --query "
    INSERT INTO dst_05030
    WITH splitByChar('.', _file) AS name_parts
    SELECT x
    FROM fileCluster('test_cluster_two_shards', '${CLICKHOUSE_DATABASE}_05030/part_*.tsv', 'TSV', 'x UInt32')
    WHERE name_parts[1] = 'part_1'
    SETTINGS parallel_distributed_insert_select = 2
"
$CLICKHOUSE_CLIENT --query "SELECT x FROM dst_05030 ORDER BY x"

# Nothing about the failed analysis must be reported as an error.
$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS text_log"
echo "--- errors logged by the interpreter ---"
$CLICKHOUSE_CLIENT --query "
    SELECT count()
    FROM system.text_log
    WHERE query_id = '${QUERY_ID}' AND logger_name = 'InterpreterInsertQuery' AND level = 'Error'
"

echo "--- alias from the SELECT list ---"
$CLICKHOUSE_CLIENT --query "
    TRUNCATE TABLE dst_05030;
    INSERT INTO dst_05030
    SELECT x * 10 AS scaled
    FROM fileCluster('test_cluster_two_shards', '${CLICKHOUSE_DATABASE}_05030/part_*.tsv', 'TSV', 'x UInt32')
    WHERE scaled > 10 AND _file = 'part_1.tsv'
    SETTINGS parallel_distributed_insert_select = 2;
    SELECT x FROM dst_05030 ORDER BY x;
"

$CLICKHOUSE_CLIENT --query "DROP TABLE dst_05030 SYNC"
rm -rf "${DATA_DIR}"
