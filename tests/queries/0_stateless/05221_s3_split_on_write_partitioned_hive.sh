#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Depends on S3

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# An insert split by size under `partition_strategy = 'hive'`: the name of the first object of every insert is
# generated anew, the objects after it are numbered from that name (`<id>.tsv`, `<id>.1.tsv`, ...), and a
# truncating insert neither overwrites nor deletes the objects of the previous inserts - the `hive` layout is
# append-only, with or without splitting, exactly as it is without `s3_split_on_write_by_size_bytes`.

PREFIX="05221_split_on_write_partitioned_hive/${CLICKHOUSE_DATABASE}"

# Every block is exactly 100 numbers of a single partition and is bigger than the limit, so every block goes
# into an object of its own (the partition column is not written into the data, so a block is a few hundred bytes).
SETTINGS="max_threads = 1, max_insert_threads = 1, max_block_size = 100, min_insert_block_size_rows = 100, min_insert_block_size_bytes = 0, s3_split_on_write_by_size_bytes = 100"
TABLE="s3(s3_conn, filename='${PREFIX}', format=TSV, structure='p String, x UInt64', partition_strategy='hive')"
OBJECTS="s3(s3_conn, filename='${PREFIX}/**.tsv', format=TSV, structure='x UInt64')"

# Per partition: the number of objects, how many of them carry a number, the rows and their sum.
SUMMARY="
    SELECT
        extract(_path, 'p=([^/]+)/') AS p,
        uniqExact(_file) AS objects,
        uniqExactIf(_file, match(_file, '^[0-9]+\\\\.[0-9]+\\\\.tsv\$')) AS numbered_objects,
        countIf(NOT match(_file, '^[0-9]+(\\\\.[0-9]+)?\\\\.tsv\$')) AS foreign_names,
        count(), sum(x)
    FROM ${OBJECTS} GROUP BY p ORDER BY p"

echo '--- The objects of an insert are numbered from the generated name of its first object'
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION ${TABLE} PARTITION BY p
        SELECT ['a', 'b.c', 'd'][intDiv(number, 400) + 1] AS p, number AS x FROM numbers(1200) SETTINGS ${SETTINGS};
    ${SUMMARY};
"

echo '--- A smaller truncating rewrite of one partition appends: nothing is overwritten or deleted'
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION ${TABLE} PARTITION BY p
        SELECT 'b.c' AS p, number AS x FROM numbers(200) SETTINGS ${SETTINGS}, s3_truncate_on_insert = 1;
    ${SUMMARY};
"

echo '--- The same without splitting, for comparison'
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION ${TABLE} PARTITION BY p
        SELECT 'a' AS p, number AS x FROM numbers(50) SETTINGS max_threads = 1, max_insert_threads = 1, s3_truncate_on_insert = 1;
    ${SUMMARY};
"
