#!/usr/bin/env bash
# Tags: zookeeper, no-parallel
# no-parallel: the databases below share one ZooKeeper path, and the recovery of the second replica
#   is what the test observes.

# A `Replicated` database whose table metadata in ZooKeeper is longer than `max_query_size` could not
# be recovered by a new replica: the recovery re-parsed that metadata with the recovering replica's
# own `max_query_size`, threw `Max query size exceeded` before creating the table and retried forever,
# while the replicas that had loaded the very same text from their disk kept serving it. The metadata
# is server-generated, so its length is not a client's business.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=none

db_zk_path="/clickhouse/databases/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/db"
db1="${CLICKHOUSE_DATABASE}_recovery_1"
db2="${CLICKHOUSE_DATABASE}_recovery_2"

${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${db1} SYNC" > /dev/null
${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${db2} SYNC" > /dev/null
${CLICKHOUSE_CLIENT} -q "CREATE DATABASE ${db1} ENGINE = Replicated('${db_zk_path}', 'shard1', 'replica1')" > /dev/null

# A comment long enough to put the table's metadata over the default `max_query_size` of 256 KiB. The
# creating session has to raise it for its own `CREATE`, which is exactly how such a table comes to be.
{
    echo -n "CREATE TABLE ${db1}.t (id UInt64, v UInt64) ENGINE = ReplicatedMergeTree ORDER BY id COMMENT '"
    head -c 300000 /dev/zero | tr '\0' 'x'
    echo "'"
} | ${CLICKHOUSE_CLIENT} --max_query_size 4194304 --distributed_ddl_output_mode none

${CLICKHOUSE_CLIENT} -q "INSERT INTO ${db1}.t SELECT number, number FROM numbers(10)"

echo -n 'the metadata in ZooKeeper is over the default limit: '
${CLICKHOUSE_CLIENT} -q "
SELECT length(value) > 262144 FROM system.zookeeper WHERE path = '${db_zk_path}/metadata' AND name = 't'"

# A second replica of the same database has to recover from that metadata. Both replicas live in one
# server here, so the recovery cannot get further than creating the table - the two would share the
# store directory of its UUID - but the parse of the metadata, which is what this is about, happens
# before that.
${CLICKHOUSE_CLIENT} -q "CREATE DATABASE ${db2} ENGINE = Replicated('${db_zk_path}', 'shard1', 'replica2')" > /dev/null

for _ in {1..120}
do
    ${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS text_log" > /dev/null
    reached_create=$(${CLICKHOUSE_CLIENT} -q "
        SELECT count() FROM system.text_log
        WHERE logger_name LIKE '%${db2}%' AND message LIKE '%Executing CREATE TABLE%'")
    if [[ "$reached_create" != "0" ]]; then
        break
    fi
    sleep 0.5
done

echo -n 'the recovery got to creating the table: '
[[ "$reached_create" != "0" ]] && echo 1 || echo 0

echo -n 'and the metadata was not refused for its size: '
${CLICKHOUSE_CLIENT} -q "
SELECT count() FROM system.text_log
WHERE logger_name LIKE '%${db2}%' AND message LIKE '%Max query size exceeded%'"

${CLICKHOUSE_CLIENT} -q "
DROP DATABASE ${db2} SYNC;
DROP DATABASE ${db1} SYNC;
" > /dev/null
