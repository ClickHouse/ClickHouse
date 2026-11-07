-- Tags: no-fasttest, need-query-parameters
-- Tag no-fasttest: depends on S3

-- A data column named `_idx` is not the position of the file in the path filter.
INSERT INTO FUNCTION file({CLICKHOUSE_DATABASE:String} || '_idx/k=1/i.csv', CSV, 'x UInt8, _idx UInt64') SELECT 1, 7;
INSERT INTO FUNCTION file({CLICKHOUSE_DATABASE:String} || '_idx/k=2/i.csv', CSV, 'x UInt8, _idx UInt64') SELECT 2, 8;
SELECT x, _idx FROM file({CLICKHOUSE_DATABASE:String} || '_idx/k=*/i.csv', CSV, 'x UInt8, _idx UInt64') WHERE _idx = 7 SETTINGS use_hive_partitioning = 0;
SELECT x, _idx FROM file({CLICKHOUSE_DATABASE:String} || '_idx/k=*/i.csv', CSV, 'x UInt8, _idx UInt64') WHERE _idx IN (0, 7, 8) ORDER BY x SETTINGS use_hive_partitioning = 0;
SELECT x, _idx FROM file({CLICKHOUSE_DATABASE:String} || '_idx/k=1/i.csv', CSV, 'x UInt8, _idx UInt64') WHERE _idx = 7 SETTINGS use_hive_partitioning = 0;

-- A Hive partition column named `_idx`.
INSERT INTO FUNCTION file({CLICKHOUSE_DATABASE:String} || '_hive/_idx=5/f.csv', CSV, 'x UInt8') SETTINGS use_hive_partitioning = 0 SELECT 1;
INSERT INTO FUNCTION file({CLICKHOUSE_DATABASE:String} || '_hive/_idx=6/f.csv', CSV, 'x UInt8') SETTINGS use_hive_partitioning = 0 SELECT 2;
SELECT x, _idx FROM file({CLICKHOUSE_DATABASE:String} || '_hive/*/f.csv', CSV, 'x UInt8') WHERE _idx = 5 SETTINGS use_hive_partitioning = 1;
SELECT x, _idx FROM file({CLICKHOUSE_DATABASE:String} || '_hive/*/f.csv', CSV, 'x UInt8') WHERE _file = 'f.csv' ORDER BY x SETTINGS use_hive_partitioning = 1;

-- Data columns named `_path` and `_idx` in an object storage glob.
INSERT INTO FUNCTION s3(s3_conn, filename = {CLICKHOUSE_DATABASE:String} || '/05358/a/data.csv', format = CSV, structure = 'x UInt8, _path String, _idx UInt64') SETTINGS s3_truncate_on_insert = 1 SELECT 5, 'zz', 9;
SELECT x, _path, _idx FROM s3(s3_conn, filename = {CLICKHOUSE_DATABASE:String} || '/05358/*/data.csv', format = CSV, structure = 'x UInt8, _path String, _idx UInt64') WHERE _path = 'zz' SETTINGS s3_path_filter_limit = 1000;
SELECT x, _path, _idx FROM s3(s3_conn, filename = {CLICKHOUSE_DATABASE:String} || '/05358/*/data.csv', format = CSV, structure = 'x UInt8, _path String, _idx UInt64') WHERE _idx = 9;
