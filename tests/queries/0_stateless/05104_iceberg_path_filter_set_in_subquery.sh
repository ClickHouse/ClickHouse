#!/usr/bin/env bash
# Tags: no-fasttest

# An Iceberg read whose `_path IN (subquery)` filter set is not prepared before the
# iterator is constructed. Two shapes reach that state: a cluster read, whose
# coordinator builds no sets, and `GLOBAL IN`, which is excluded from the eager build
# on purpose. In both, the iterator and the reader of the same query prepare the same
# set, which used to happen on two threads at once.
# The assertions here are deterministic; the concurrency is covered by the sanitizer
# and stress CI arms, which run this file too.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}"
TABLE_PATH="${CLICKHOUSE_USER_FILES}/${TABLE}/"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
rm -rf "${TABLE_PATH}"

${CLICKHOUSE_CLIENT} --query "
    SET allow_experimental_insert_into_iceberg = 1;
    CREATE TABLE ${TABLE} (part Int64, v String)
        ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')
        PARTITION BY (part);
"

# One insert per partition, so the snapshot has several data manifests and manifest
# decoding has real work to overlap with the set build.
for i in 1 2 3 4; do
    ${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query \
        "INSERT INTO ${TABLE} SELECT ${i}, 'v${i}'"
done

echo "cluster, path filter:"
# Pinned: the old analyzer prepares no set for this shape, so the coordinator filters every
# object out, the shards receive no files, and this arm exercises no concurrency at all.
${CLICKHOUSE_CLIENT} --query "
    SELECT count(), arraySort(groupArray(v))
    FROM icebergLocalCluster('test_shard_localhost', '${TABLE_PATH}', 'Parquet')
    WHERE _path IN (SELECT _path FROM ${CLICKHOUSE_DATABASE}.${TABLE} WHERE part IN (2, 3))
    SETTINGS enable_analyzer = 1
"

echo "global in, path filter:"
${CLICKHOUSE_CLIENT} --query "
    SELECT count(), arraySort(groupArray(v))
    FROM icebergLocal('${TABLE_PATH}', 'Parquet')
    WHERE _path GLOBAL IN (SELECT _path FROM ${CLICKHOUSE_DATABASE}.${TABLE} WHERE part IN (2, 3))
"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
rm -rf "${TABLE_PATH}"
