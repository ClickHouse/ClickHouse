#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Depends on S3

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# An `S3` table keeps no metadata about the objects it has written: `DETACH` / `ATTACH` or a server restart
# rebuilds its list of the paths from the single configured key, so the numbered objects of an earlier insert
# split by size are not attributable to the table anymore.
#
# `TRUNCATE TABLE` therefore leaves such a forgotten tail where it is - deleting an object the table does not
# own would be destructive - and only warns about it in the server log. A truncating insert split by size does
# reclaim the tail: it overwrites the whole numbered sequence of the key anyway, so it removes the leftovers
# by number instead of failing on the keys that are taken.

PREFIX="05234_split_truncate_after_reload/${CLICKHOUSE_DATABASE}"

# Every block is exactly 100 numbers, and a new object is started as soon as 1000 bytes are written,
# so the resulting objects are the same on every run.
SETTINGS="max_threads = 1, max_insert_threads = 1, max_block_size = 100, min_insert_block_size_rows = 100, min_insert_block_size_bytes = 0, s3_split_on_write_by_size_bytes = 1000"

${CLICKHOUSE_CLIENT} --query "
    SELECT '--- A split insert';
    CREATE TABLE test_05234 (x UInt64) ENGINE = S3(s3_conn, filename='${PREFIX}/data.tsv', format=TSV);
    INSERT INTO test_05234 SELECT number FROM numbers(1000) SETTINGS ${SETTINGS};
    SELECT count(), min(x), max(x) FROM test_05234;
    SELECT _file FROM s3(s3_conn, filename='${PREFIX}/data*.tsv', format=TSV, structure='x UInt64') GROUP BY _file ORDER BY _file;

    SELECT '--- After a reload the table reads only the base object';
    DETACH TABLE test_05234;
    ATTACH TABLE test_05234;
    SELECT count(), min(x), max(x) FROM test_05234;
"

echo '--- The truncate removes the base object and leaves the forgotten tail in place, with a warning in the log'
${CLICKHOUSE_CLIENT} --send_logs_level=warning --query "TRUNCATE TABLE test_05234" 2>&1 \
    | grep -c -F "has left the object ${PREFIX}/data.1.tsv in place"

${CLICKHOUSE_CLIENT} --query "
    SELECT _file FROM s3(s3_conn, filename='${PREFIX}/data*.tsv', format=TSV, structure='x UInt64') GROUP BY _file ORDER BY _file;

    SELECT '--- A truncating split insert reclaims the forgotten tail by number instead of failing on it';
    INSERT INTO test_05234 SELECT number FROM numbers(300) SETTINGS ${SETTINGS}, s3_truncate_on_insert = 1;
    SELECT count(), min(x), max(x) FROM test_05234;
    SELECT _file FROM s3(s3_conn, filename='${PREFIX}/data*.tsv', format=TSV, structure='x UInt64') GROUP BY _file ORDER BY _file;
    DROP TABLE test_05234;
"
