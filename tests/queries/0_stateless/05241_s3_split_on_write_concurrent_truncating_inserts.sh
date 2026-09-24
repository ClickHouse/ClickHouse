#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Depends on S3

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Two truncating inserts into the same object storage table must not run at the same time. Both would keep
# the base key, and once they roll over into the numbered keys, which are reserved one by one, they would get
# disjoint tails: the insert committed last would win only the base object, and the table would read the
# numbered objects of both. A truncating insert fails instead, before it deletes anything, when the key it
# starts with is being written by another insert into the table.

PREFIX="05241_split_concurrent_truncating_inserts/${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION s3(s3_conn, filename='${PREFIX}/data.tsv', format=TSV) SELECT 0 SETTINGS s3_truncate_on_insert = 1;
    CREATE TABLE test_05241 (x UInt64) ENGINE = S3(s3_conn, filename='${PREFIX}/data.tsv', format=TSV);
"

# This insert rolls over on every block of 20 rows. Every row takes 50 ms, so it lasts about ten seconds.
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO test_05241 SELECT number + 1000000 FROM numbers(200) WHERE NOT sleepEachRow(0.05)
    SETTINGS max_threads = 1, max_insert_threads = 1, max_block_size = 20, min_insert_block_size_rows = 20, min_insert_block_size_bytes = 0,
        s3_split_on_write_by_size_bytes = 40, s3_truncate_on_insert = 1;
" &

echo '--- An object of the running insert is published'
for _ in $(seq 1 300)
do
    published=$(${CLICKHOUSE_CLIENT} --query "SELECT count() > 0 FROM test_05241 WHERE x >= 1000000")
    if [ "$published" = "1" ]
    then
        break
    fi
    sleep 0.1
done
echo "$published"

echo '--- The second truncating insert is refused while the first one is running'
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO test_05241 SELECT number + 2000000 FROM numbers(200)
    SETTINGS max_block_size = 20, min_insert_block_size_rows = 20, min_insert_block_size_bytes = 0,
        s3_split_on_write_by_size_bytes = 40, s3_truncate_on_insert = 1;
" 2>&1 | grep -c -m1 'is being written by a concurrent insert into the table'

wait

echo '--- Only the rows of the first insert are there'
${CLICKHOUSE_CLIENT} --query "
    SELECT count(), uniqExact(x), min(x), max(x)
    FROM s3(s3_conn, filename='${PREFIX}/data*.tsv', format=TSV, structure='x UInt64');
"
${CLICKHOUSE_CLIENT} --query "SELECT count(), uniqExact(x), min(x), max(x) FROM test_05241"

echo '--- Once the first insert is over, a truncating insert rewrites the table'
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO test_05241 SELECT number + 2000000 FROM numbers(10) SETTINGS s3_truncate_on_insert = 1;
"
${CLICKHOUSE_CLIENT} --query "
    SELECT count(), min(x), max(x) FROM s3(s3_conn, filename='${PREFIX}/data*.tsv', format=TSV, structure='x UInt64');
"
${CLICKHOUSE_CLIENT} --query "SELECT count(), min(x), max(x) FROM test_05241"

${CLICKHOUSE_CLIENT} --query "DROP TABLE test_05241"
