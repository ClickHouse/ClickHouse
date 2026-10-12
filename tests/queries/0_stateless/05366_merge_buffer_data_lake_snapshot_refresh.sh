#!/usr/bin/env bash
# Tags: no-fasttest

# Reads of a data lake table through `merge()`, `Merge` and `Buffer` must see commits made after
# the last direct query of the table. `t_writer` is a second table object over the same Iceberg
# table, standing in for an external writer.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DIR="${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/"
trap 'rm -rf "${DIR}"' EXIT

$CLICKHOUSE_CLIENT --allow_insert_into_iceberg 1 --allow_experimental_iceberg_compaction 0 --iceberg_delete_data_on_drop 0 -m -q "
CREATE TABLE t_lake (k Int64) ENGINE = IcebergLocal('${DIR}', 'Parquet');
INSERT INTO t_lake SELECT number FROM numbers(100);
CREATE TABLE t_writer ENGINE = IcebergLocal('${DIR}', 'Parquet');
CREATE TABLE t_merge ENGINE = Merge(currentDatabase(), '^t_lake\$');
CREATE TABLE t_nested ENGINE = Merge(currentDatabase(), '^t_merge\$');
CREATE TABLE t_alias ENGINE = Alias(t_lake);
CREATE TABLE t_buffer (k Int64) ENGINE = Buffer(currentDatabase(), t_lake, 1, 100000, 100000, 1000000000, 1000000000, 1000000000000, 1000000000000);

SELECT 'direct', count(), sum(k) FROM t_lake;

INSERT INTO t_writer SELECT number + 100 FROM numbers(10);
SELECT 'merge()', count(), sum(k) FROM merge(currentDatabase(), '^t_lake\$');
INSERT INTO t_writer SELECT number + 200 FROM numbers(10);
SELECT 'Merge', count(), sum(k) FROM t_merge;
INSERT INTO t_writer SELECT number + 300 FROM numbers(10);
SELECT 'Merge over Merge', count(), sum(k) FROM t_nested;
INSERT INTO t_writer SELECT number + 400 FROM numbers(10);
SELECT 'merge() over Alias', count(), sum(k) FROM merge(currentDatabase(), '^t_alias\$');
INSERT INTO t_writer SELECT number + 500 FROM numbers(10);
SELECT 'Buffer', count(), sum(k) FROM t_buffer;

ALTER TABLE t_writer RENAME COLUMN k TO v;
DESCRIBE TABLE merge(currentDatabase(), '^t_lake\$') SETTINGS describe_compact_output = 1;
INSERT INTO t_writer SELECT number + 600 FROM numbers(10);
SELECT 'merge() after rename', count(), sum(v) FROM merge(currentDatabase(), '^t_lake\$');

DROP TABLE t_buffer;
DROP TABLE t_alias;
DROP TABLE t_nested;
DROP TABLE t_merge;
DROP TABLE t_writer;
DROP TABLE t_lake;
"
