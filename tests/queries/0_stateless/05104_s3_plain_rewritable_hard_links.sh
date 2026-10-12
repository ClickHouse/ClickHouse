#!/usr/bin/env bash
# Tags: no-fasttest, no-shared-merge-tree, no-replicated-database
# Tag no-fasttest: requires S3
# Tag no-shared-merge-tree: does not support replication
# Tag no-replicated-database: plain rewritable should not be shared between replicas

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Mutations and ALTERs on a plain_rewritable disk with `enable_hard_links` hard-link the untouched files of a part
# into the mutated part, so the two parts share the blobs. A readonly reader on the same endpoint sees the mutated data.

endpoint="http://localhost:11111/test/${CLICKHOUSE_TEST_UNIQUE_NAME}/"
disk_args="type = s3_plain_rewritable, endpoint = '${endpoint}', access_key_id = clickhouse, secret_access_key = clickhouse, enable_hard_links = 1"

${CLICKHOUSE_CLIENT} -m --query "
DROP TABLE IF EXISTS writer SYNC;
DROP TABLE IF EXISTS reader SYNC;

CREATE TABLE writer (key Int32, value String) ENGINE = MergeTree ORDER BY key
SETTINGS table_disk = 1, disk = disk(name = '${CLICKHOUSE_TEST_UNIQUE_NAME}_writer', ${disk_args}),
         min_bytes_for_wide_part = 0, old_parts_lifetime = 600;

INSERT INTO writer SELECT number, toString(number) FROM numbers(100);
INSERT INTO writer SELECT number, toString(number) FROM numbers(100, 100);
SELECT count(), sum(key) FROM writer;
"

echo '-- update (mutation, unchanged columns are hard-linked)'
${CLICKHOUSE_CLIENT} -m --query "
ALTER TABLE writer UPDATE value = concat(value, '!') WHERE key % 2 = 0 SETTINGS mutations_sync = 1;
SELECT count(), countIf(endsWith(value, '!')) FROM writer;
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'writer' AND active;
"

echo '-- add a materialized column, drop a column, rename a column'
${CLICKHOUSE_CLIENT} -m --query "
ALTER TABLE writer ADD COLUMN doubled UInt32;
ALTER TABLE writer UPDATE doubled = key * 2 WHERE 1 SETTINGS mutations_sync = 1;
ALTER TABLE writer DROP COLUMN value SETTINGS mutations_sync = 1;
ALTER TABLE writer RENAME COLUMN doubled TO renamed SETTINGS mutations_sync = 1;
SELECT count(), sum(key), sum(renamed) FROM writer;
"

echo '-- a readonly reader on the same endpoint sees the mutated data'
${CLICKHOUSE_CLIENT} -m --query "
CREATE TABLE reader (key Int32, renamed UInt32) ENGINE = MergeTree ORDER BY key
SETTINGS table_disk = 1, disk = disk(readonly = true, name = '${CLICKHOUSE_TEST_UNIQUE_NAME}_reader', ${disk_args});
SELECT count(), sum(key), sum(renamed) FROM reader;
SELECT count() FROM (SELECT * FROM writer EXCEPT SELECT * FROM reader);
SELECT count() FROM (SELECT * FROM reader EXCEPT SELECT * FROM writer);
"

echo '-- merge after the mutations'
${CLICKHOUSE_CLIENT} -m --query "
OPTIMIZE TABLE writer FINAL;
SELECT count(), sum(key), sum(renamed) FROM writer;
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'writer' AND active;
"

echo '-- dropping the tables removes all the objects'
${CLICKHOUSE_CLIENT} -m --query "
DROP TABLE reader SYNC;
DROP TABLE writer SYNC;
SELECT count() FROM s3('${endpoint}**', 'clickhouse', 'clickhouse', 'One');
"
