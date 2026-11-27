#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Depends on S3

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A truncating insert deletes the split objects of the previous inserts into the table. The objects of an
# insert that is still running are not leftovers of a previous insert: they are kept, together with the
# objects that insert writes afterwards, and the truncating insert does not reuse their keys either.

PREFIX="05238_split_truncate_during_insert/${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION s3(s3_conn, filename='${PREFIX}/data.tsv', format=TSV) SELECT 0 SETTINGS s3_truncate_on_insert = 1;
    CREATE TABLE test_05238 (x UInt64) ENGINE = S3(s3_conn, filename='${PREFIX}/data.tsv', format=TSV);
"

# The base object is taken, so this insert steps aside into the numbered keys and rolls over on every block
# of 20 rows. Every row takes 50 ms, so a block is published about once a second, for ten seconds.
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO test_05238 SELECT number + 1000000 FROM numbers(200) WHERE NOT sleepEachRow(0.05)
    SETTINGS max_threads = 1, max_insert_threads = 1, max_block_size = 20, min_insert_block_size_rows = 20, min_insert_block_size_bytes = 0,
        s3_split_on_write_by_size_bytes = 40, s3_create_new_file_on_insert = 1;
" &

echo '--- A shard of the running insert is published'
for _ in $(seq 1 300)
do
    published=$(${CLICKHOUSE_CLIENT} --query "SELECT count() > 0 FROM test_05238 WHERE x >= 1000000")
    if [ "$published" = "1" ]
    then
        break
    fi
    sleep 0.1
done
echo "$published"

# The rewrite of the base object happens while the insert above is still running: it has about nine
# more seconds to go, and this one is over in a fraction of a second.
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO test_05238 SELECT number + 2000000 FROM numbers(10) SETTINGS s3_truncate_on_insert = 1;
"

wait

echo '--- The truncating insert rewrote the base object, the running insert kept every row'
${CLICKHOUSE_CLIENT} --query "
    SELECT count(), uniqExact(x), countIf(x < 2000000), countIf(x >= 2000000)
    FROM s3(s3_conn, filename='${PREFIX}/data*.tsv', format=TSV, structure='x UInt64');
"

echo '--- The table reads all of them'
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM test_05238"

${CLICKHOUSE_CLIENT} --query "DROP TABLE test_05238"
