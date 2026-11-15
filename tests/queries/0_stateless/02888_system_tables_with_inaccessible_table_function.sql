-- Tags: no-fasttest

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE:Identifier};

CREATE DATABASE {CLICKHOUSE_DATABASE:Identifier};


CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.tablefunc01 (x int) AS postgresql('127.121.0.1:5432', 'postgres_db', 'postgres_table', 'postgres_user', '124444');
CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.tablefunc02 (x int) AS mysql('127.123.0.1:3306', 'mysql_db', 'mysql_table', 'mysql_user','123123');
CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.tablefunc03 (a int) AS sqlite('db_path', 'table_name');
CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.tablefunc04 (a int) AS mongodb('127.0.0.1:27017','test', 'my_collection', 'test_user', 'password', 'a Int');
CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.tablefunc05 (a int) AS redis('127.0.0.1:6379', 'key', 'key UInt32');
CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.tablefunc06 (a int) AS s3('http://some_addr:9000/cloud-storage-01/data.tsv', 'M9O7o0SX5I4udXhWxI12', '9ijqzmVN83fzD9XDkEAAAAAAAA', 'TSV');
-- No format argument, so resolving this table function has to infer the format from the endpoint.
CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.tablefunc07 (x String) AS url('http://some_addr:9000/nonexistent');


CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.tablefunc01_without_schema AS postgresql('127.121.0.1:5432', 'postgres_db', 'postgres_table', 'postgres_user', '124444'); -- { serverError POSTGRESQL_CONNECTION_FAILURE }
CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.tablefunc02_without_schema AS mysql('127.123.0.1:3306', 'mysql_db', 'mysql_table', 'mysql_user','123123'); -- {serverError ALL_CONNECTION_TRIES_FAILED }

SELECT name, engine, engine_full, create_table_query, data_paths, notEmpty([metadata_path]), notEmpty([uuid])
    FROM system.tables
    WHERE name like '%tablefunc%' and database=currentDatabase()
    ORDER BY name;

DETACH TABLE {CLICKHOUSE_DATABASE:Identifier}.tablefunc01;
DETACH TABLE {CLICKHOUSE_DATABASE:Identifier}.tablefunc02;
DETACH TABLE {CLICKHOUSE_DATABASE:Identifier}.tablefunc03;
DETACH TABLE {CLICKHOUSE_DATABASE:Identifier}.tablefunc04;
DETACH TABLE {CLICKHOUSE_DATABASE:Identifier}.tablefunc05;
DETACH TABLE {CLICKHOUSE_DATABASE:Identifier}.tablefunc06;
DETACH TABLE {CLICKHOUSE_DATABASE:Identifier}.tablefunc07;

ATTACH TABLE {CLICKHOUSE_DATABASE:Identifier}.tablefunc01;
ATTACH TABLE {CLICKHOUSE_DATABASE:Identifier}.tablefunc02;
ATTACH TABLE {CLICKHOUSE_DATABASE:Identifier}.tablefunc03;
ATTACH TABLE {CLICKHOUSE_DATABASE:Identifier}.tablefunc04;
ATTACH TABLE {CLICKHOUSE_DATABASE:Identifier}.tablefunc05;
ATTACH TABLE {CLICKHOUSE_DATABASE:Identifier}.tablefunc06;
ATTACH TABLE {CLICKHOUSE_DATABASE:Identifier}.tablefunc07;

SELECT name, engine, engine_full, create_table_query, data_paths, notEmpty([metadata_path]), notEmpty([uuid])
    FROM system.tables
    WHERE name like '%tablefunc%' and database=currentDatabase()
    ORDER BY name;

SELECT count() FROM {CLICKHOUSE_DATABASE:Identifier}.tablefunc01; -- { serverError POSTGRESQL_CONNECTION_FAILURE }
SELECT engine FROM system.tables WHERE name = 'tablefunc01' and database=currentDatabase();

-- Asking a table nobody named for a lock or for its size must not resolve its table function.
SYSTEM STOP MERGES {CLICKHOUSE_DATABASE:Identifier}.tablefunc07;
SYSTEM START MERGES {CLICKHOUSE_DATABASE:Identifier}.tablefunc07;
SELECT lifetime_rows, lifetime_bytes FROM system.tables WHERE database = currentDatabase() AND name = 'tablefunc07';
SELECT name, data_compressed_bytes, data_uncompressed_bytes, marks_bytes
    FROM system.columns WHERE database = currentDatabase() AND table = 'tablefunc07';

-- Once the table function is resolved, the nested storage answers again.
CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.mem (x UInt64) ENGINE = Memory;
INSERT INTO {CLICKHOUSE_DATABASE:Identifier}.mem SELECT number FROM numbers(5);
CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.tablefunc08 (x UInt64) AS merge(currentDatabase(), '^mem$');
SELECT total_rows,
       total_bytes = (SELECT total_bytes FROM system.tables
                      WHERE database = currentDatabase() AND name = 'mem')
    FROM system.tables WHERE database = currentDatabase() AND name = 'tablefunc08';
SELECT count() FROM {CLICKHOUSE_DATABASE:Identifier}.tablefunc08;
SELECT total_rows,
       total_bytes = (SELECT total_bytes FROM system.tables
                      WHERE database = currentDatabase() AND name = 'mem')
    FROM system.tables WHERE database = currentDatabase() AND name = 'tablefunc08';

-- A resolved table function answers with the nested storage's own data paths.
INSERT INTO FUNCTION file(currentDatabase() || '_02888_data_paths.tsv', TSVWithNamesAndTypes, 'x UInt64')
    SELECT number FROM numbers(3) SETTINGS engine_file_truncate_on_insert = 1;
-- No structure argument, so the nested storage is built lazily and the header supplies both the
-- column name and its type.
CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.tablefunc09 (x UInt64) AS file(currentDatabase() || '_02888_data_paths.tsv', TSVWithNamesAndTypes);
SELECT length(data_paths) = 1
    AND basename(data_paths[1]) = currentDatabase() || '_02888_data_paths.tsv'
    FROM system.tables WHERE database = currentDatabase() AND name = 'tablefunc09';
SELECT count() FROM {CLICKHOUSE_DATABASE:Identifier}.tablefunc09;
SELECT length(data_paths) = 1
    AND basename(data_paths[1]) = currentDatabase() || '_02888_data_paths.tsv'
    FROM system.tables WHERE database = currentDatabase() AND name = 'tablefunc09';

-- A never resolved proxy reports an unknown row count, so the emptiness interlock no longer
-- refuses it. Only the proxy's own name goes away; the rows it would have read are untouched.
CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.tablefunc10 (x UInt64) AS merge(currentDatabase(), '^mem$');
-- ignore_drop_queries_probability = 0: the stress runner sets it, and because this table stores no
-- data on disk a rewritten DROP becomes a TRUNCATE that resolves the table function.
DROP TABLE IF EMPTY {CLICKHOUSE_DATABASE:Identifier}.tablefunc10 SETTINGS ignore_drop_queries_probability = 0;
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'tablefunc10';
SELECT count() FROM {CLICKHOUSE_DATABASE:Identifier}.mem;

-- The column sizes of a resolved proxy come from the nested storage. Only a wide part tracks them
-- per column, so a compact one would report zero either way.
CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.sizes_src (x String) ENGINE = MergeTree ORDER BY x
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO {CLICKHOUSE_DATABASE:Identifier}.sizes_src SELECT toString(number) FROM numbers(10000);
SELECT data_compressed_bytes > 0 FROM system.columns
    WHERE database = currentDatabase() AND table = 'sizes_src';
CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.tablefunc11 (x String) AS merge(currentDatabase(), '^sizes_src$');
SELECT data_compressed_bytes > 0 FROM system.columns
    WHERE database = currentDatabase() AND table = 'tablefunc11';
SELECT count() FROM {CLICKHOUSE_DATABASE:Identifier}.tablefunc11;
SELECT (data_compressed_bytes, data_uncompressed_bytes, marks_bytes)
     = (SELECT (data_compressed_bytes, data_uncompressed_bytes, marks_bytes) FROM system.columns
        WHERE database = currentDatabase() AND table = 'sizes_src' AND name = 'x')
    FROM system.columns WHERE database = currentDatabase() AND table = 'tablefunc11';

-- Not covered: `lifetime_rows`/`lifetime_bytes` (only `Buffer` reports them), a forwarded lock
-- (only a `timeSeries*` target holds one), `tryGetColumnSizes` (no proxy overrides it) and the
-- `StorageTableProxy` stand-in of `lazy_load_tables`.

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE:Identifier};
