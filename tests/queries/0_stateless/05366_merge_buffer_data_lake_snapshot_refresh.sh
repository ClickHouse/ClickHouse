#!/usr/bin/env bash
# Tags: no-fasttest

# Reads of a data lake table through `merge()`, `Merge` and `Buffer` must see commits made after
# the last direct query of the table. `t_writer` is a second table object over the same Iceberg
# table, standing in for an external writer.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DIR="${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/"
DIR_LAZY="${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}_lazy/"
DB_LAZY="${CLICKHOUSE_DATABASE}_lazy"
trap 'rm -rf "${DIR}" "${DIR_LAZY}"' EXIT

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

ALTER TABLE t_writer RENAME COLUMN v TO w;
DESCRIBE TABLE loop(currentDatabase(), 't_lake') SETTINGS describe_compact_output = 1;
ALTER TABLE t_writer RENAME COLUMN w TO u;
SELECT 'loop() after rename', sum(u) FROM (SELECT u FROM loop(currentDatabase(), 't_lake') LIMIT 160);

DROP TABLE t_buffer;
DROP TABLE t_alias;
DROP TABLE t_nested;
DROP TABLE t_merge;
DROP TABLE t_writer;
DROP TABLE t_lake;
"

# In a database with `lazy_load_tables` the catalog holds a stand-in, and an `INSERT` pins the snapshot
# of the storage behind it. `DETACH` / `ATTACH DATABASE` installs the stand-in, as a restart does.
$CLICKHOUSE_CLIENT --allow_insert_into_iceberg 1 --allow_experimental_iceberg_compaction 0 --iceberg_delete_data_on_drop 0 -m -q "
DROP DATABASE IF EXISTS ${DB_LAZY};
CREATE DATABASE ${DB_LAZY} ENGINE = Atomic SETTINGS lazy_load_tables = 1;
CREATE TABLE ${DB_LAZY}.t_lake (k Int64) ENGINE = IcebergLocal('${DIR_LAZY}', 'Parquet');
INSERT INTO ${DB_LAZY}.t_lake SELECT number FROM numbers(100);
CREATE TABLE ${DB_LAZY}.t_merge ENGINE = Merge('${DB_LAZY}', '^t_lake\$');
CREATE TABLE ${DB_LAZY}.t_buffer (k Int64) ENGINE = Buffer('${DB_LAZY}', t_lake, 1, 100000, 100000, 1000000000, 1000000000, 1000000000000, 1000000000000);
CREATE TABLE ${DB_LAZY}.t_alias ENGINE = Alias('${DB_LAZY}', 't_lake');
CREATE TABLE ${DB_LAZY}.t_writer ENGINE = IcebergLocal('${DIR_LAZY}', 'Parquet');
DETACH DATABASE ${DB_LAZY};
ATTACH DATABASE ${DB_LAZY};
SELECT 'lazy', engine FROM system.tables WHERE database = '${DB_LAZY}' AND name = 't_lake';

INSERT INTO ${DB_LAZY}.t_lake SELECT number + 100 FROM numbers(10);
SELECT 'lazy merge()', count(), sum(k) FROM merge('${DB_LAZY}', '^t_lake\$');
INSERT INTO ${DB_LAZY}.t_lake SELECT number + 200 FROM numbers(10);
SELECT 'lazy Merge', count(), sum(k) FROM ${DB_LAZY}.t_merge;
INSERT INTO ${DB_LAZY}.t_lake SELECT number + 300 FROM numbers(10);
SELECT 'lazy Buffer', count(), sum(k) FROM ${DB_LAZY}.t_buffer;
INSERT INTO ${DB_LAZY}.t_lake SELECT number + 400 FROM numbers(10);
SELECT 'lazy merge() over Alias', count(), sum(k) FROM merge('${DB_LAZY}', '^t_alias\$');

-- A stand-in that is not loaded yet reports the columns of its \`CREATE\` query.
ALTER TABLE ${DB_LAZY}.t_writer RENAME COLUMN k TO v;
INSERT INTO ${DB_LAZY}.t_writer SELECT number + 500 FROM numbers(10);
DETACH DATABASE ${DB_LAZY};
ATTACH DATABASE ${DB_LAZY};
DESCRIBE TABLE merge('${DB_LAZY}', '^t_lake\$') SETTINGS describe_compact_output = 1;
SELECT 'lazy merge() after rename', count(), sum(v) FROM merge('${DB_LAZY}', '^t_lake\$');
DETACH DATABASE ${DB_LAZY};
ATTACH DATABASE ${DB_LAZY};
SELECT 'lazy merge() over Alias after rename', count(), sum(v) FROM merge('${DB_LAZY}', '^t_alias\$');

DROP DATABASE ${DB_LAZY};
"
