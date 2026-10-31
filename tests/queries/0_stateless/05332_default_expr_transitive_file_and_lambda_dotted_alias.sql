-- A requested column missing from a file is computed from its `DEFAULT`, which may read another
-- defaulted column that is missing from the file as well. The physical inputs of the whole chain
-- must be read, otherwise the chain is evaluated over type defaults.
-- The file is named after the test database, so that concurrent runs of the test do not share it.
INSERT INTO FUNCTION file({CLICKHOUSE_DATABASE:String}, 'JSONEachRow', 'a UInt8, z UInt8') SELECT 10, 1 SETTINGS engine_file_truncate_on_insert = 1;

DROP TABLE IF EXISTS t_05332_chain;
CREATE TABLE t_05332_chain (a UInt8, z UInt8, c UInt8 DEFAULT a + 1, b UInt8 DEFAULT c + 1)
ENGINE = File(JSONEachRow, {CLICKHOUSE_DATABASE:String});
SELECT b FROM t_05332_chain;
SELECT z, b FROM t_05332_chain;
DROP TABLE t_05332_chain;

DROP TABLE IF EXISTS t_05332_chain_matcher;
CREATE TABLE t_05332_chain_matcher (a UInt8, z UInt8, c UInt8 DEFAULT a + 1, b UInt8 DEFAULT plus(COLUMNS('^c$'), 1))
ENGINE = File(JSONEachRow, {CLICKHOUSE_DATABASE:String});
SELECT b FROM t_05332_chain_matcher;
DROP TABLE t_05332_chain_matcher;

-- Inside `x -> x.id` the identifier `x.id` is a field access of the lambda parameter,
-- not a reference to a column or an alias named `x.id`.
DROP TABLE IF EXISTS t_05332_dotted;
CREATE TABLE t_05332_dotted (`x.id` UInt8 ALIAS 7, arr Array(Tuple(id UInt8))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_05332_dotted (arr) VALUES ([(1), (2)]);
SELECT arrayMap(x -> x.id, arr) FROM t_05332_dotted SETTINGS optimize_respect_aliases = 1, enable_analyzer = 0;
SELECT arrayMap(x -> x.id, arr) FROM t_05332_dotted SETTINGS optimize_respect_aliases = 1, enable_analyzer = 1;
SELECT `x.id`, arrayMap(y -> y.id, arr) FROM t_05332_dotted SETTINGS optimize_respect_aliases = 1;
DROP TABLE t_05332_dotted;

WITH 7 AS `x.id` SELECT arrayMap(x -> x.id, [CAST(tuple(1), 'Tuple(id UInt8)')]) SETTINGS enable_analyzer = 0;
WITH 7 AS `x.id` SELECT arrayMap(x -> x.id, [CAST(tuple(1), 'Tuple(id UInt8)')]) SETTINGS enable_analyzer = 1;
WITH 7 AS `x.id` SELECT `x.id`, arrayMap(y -> y.id, [CAST(tuple(1), 'Tuple(id UInt8)')]) SETTINGS enable_analyzer = 0;
