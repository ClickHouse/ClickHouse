#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Depends on S3

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Once an insert into a plain `S3` table has committed the object of the starting key, the table keeps the key
# as taken, and does not rely on the object storage alone: not every S3 implementation reports an object right
# after it has been written. The next insert with `s3_create_new_file_on_insert` steps aside into `data.1.tsv`
# instead of overwriting `data.tsv`. A stale negative answer of the object storage is emulated by removing the
# object behind the back of the table - through another table over the same key.
# `TRUNCATE TABLE` removes the object of the starting key, and the key is free again for a plain insert.

PREFIX="05291_committed_starting_key/${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE test_05291 (x UInt64) ENGINE = S3(s3_conn, filename='${PREFIX}/data.tsv', format=TSV);
    CREATE TABLE test_05291_other (x UInt64) ENGINE = S3(s3_conn, filename='${PREFIX}/data.tsv', format=TSV);

    SELECT '--- A plain insert commits the starting key';
    INSERT INTO test_05291 VALUES (1), (2), (3);
    SELECT _file, count() FROM s3(s3_conn, filename='${PREFIX}/data*.tsv', format=TSV, structure='x UInt64') GROUP BY _file ORDER BY _file;

    SELECT '--- The object storage does not report the object anymore';
    TRUNCATE TABLE test_05291_other;
    SELECT count() FROM s3(s3_conn, filename='${PREFIX}/data*.tsv', format=TSV, structure='x UInt64');

    SELECT '--- The next insert steps aside from the committed starting key';
    INSERT INTO test_05291 SETTINGS s3_create_new_file_on_insert = 1 VALUES (4), (5);
    SELECT _file, count() FROM s3(s3_conn, filename='${PREFIX}/data*.tsv', format=TSV, structure='x UInt64') GROUP BY _file ORDER BY _file;

    SELECT '--- The table is truncated: the starting key is free again';
    TRUNCATE TABLE test_05291;
    INSERT INTO test_05291 VALUES (6);
    SELECT _file, count() FROM s3(s3_conn, filename='${PREFIX}/data*.tsv', format=TSV, structure='x UInt64') GROUP BY _file ORDER BY _file;
    SELECT * FROM test_05291;

    DROP TABLE test_05291;
    DROP TABLE test_05291_other;
"
