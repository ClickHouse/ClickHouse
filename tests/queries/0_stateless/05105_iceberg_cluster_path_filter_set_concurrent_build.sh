#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires the IcebergLocal engine (USE_AVRO build option).
#
# A cluster read of an Iceberg table filtered by `_path IN (subquery)`. The coordinator builds no
# set for that shape, so on the shard the Iceberg manifest producer thread and the query thread
# reach the same unbuilt set at the same time: one builds it for the path/file filter, the other
# for manifest pruning. A plain local read of the same shape does not reach that pair: its set is
# already built by the time either iterator is constructed. The assertions below are
# deterministic; the concurrency oracle is CI's sanitizer and stress arms.
#
# The two settings pinned in the final SELECT are the manifest-pruning builder's gates: with
# use_iceberg_partition_pruning = 0 there is no manifest filter DAG, and with
# use_index_for_in_with_subqueries = 0 the ordered build returns before its lock. Either way only
# one builder reaches the set, and this test then exercises nothing while still printing its
# expected output, so keep both pins.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_ice_pathset_${CLICKHOUSE_DATABASE}_${RANDOM}"
PATHS="paths_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

trap "rm -rf '${TABLE_PATH}'" EXIT

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
rm -rf "${TABLE_PATH}"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (part UInt32, v UInt32)
    ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')
"

# One INSERT per partition, so the snapshot carries several data manifests to decode.
for p in 1 2 3 4 5 6; do
    ${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --max_threads=1 --query "
        INSERT INTO ${TABLE} SELECT ${p} AS part, ${p} * 100 + number AS v FROM numbers(10)
    "
done

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${PATHS}"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${PATHS} (p String) ENGINE = MergeTree ORDER BY p"
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO ${PATHS} SELECT DISTINCT _path FROM ${TABLE} WHERE part IN (2, 3)
"

# The subquery is slow on purpose: whichever thread builds the set first holds the build long
# enough for the other to arrive while it is still running.
${CLICKHOUSE_CLIENT} --query "
SELECT count(), sum(v)
FROM icebergLocalCluster('test_shard_localhost', '${TABLE_PATH}', 'Parquet')
WHERE _path IN (SELECT p FROM ${CLICKHOUSE_DATABASE}.${PATHS} WHERE NOT ignore(sleepEachRow(0.4)))
SETTINGS enable_analyzer = 1, iceberg_manifest_decode_concurrency = 4,
    use_iceberg_partition_pruning = 1, use_index_for_in_with_subqueries = 1
"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${PATHS}"
