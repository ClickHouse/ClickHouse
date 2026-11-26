-- Tags: no-parallel-replicas
DROP TABLE IF EXISTS t1;
CREATE TABLE t1 (a UInt32, b String) ENGINE = MergeTree ORDER BY a
    AS INSERT SELECT number AS a, toString(number) AS b FROM numbers(3);
SELECT a, b FROM t1 ORDER BY a;
DROP TABLE t1;

DROP TABLE IF EXISTS t2;
CREATE TABLE t2 ENGINE = MergeTree ORDER BY a
    AS INSERT SELECT number AS a, toString(number) AS b FROM numbers(3);
SELECT a, b FROM t2 ORDER BY a;
DROP TABLE t2;

DROP TABLE IF EXISTS t3;
CREATE TABLE t3 (a UInt32, b String) ENGINE = MergeTree ORDER BY a
    AS INSERT VALUES (1, 'hello'), (2, 'world');
SELECT a, b FROM t3 ORDER BY a;
DROP TABLE t3;

DROP TABLE IF EXISTS t4;
CREATE TABLE t4 (a UInt32) ENGINE = MergeTree ORDER BY a;
CREATE TABLE t4 (a UInt32) ENGINE = MergeTree ORDER BY a
    AS INSERT VALUES (1); -- { serverError TABLE_ALREADY_EXISTS }
DROP TABLE t4;

DROP TABLE IF EXISTS t5;
CREATE TABLE IF NOT EXISTS t5 (a UInt32, b String) ENGINE = MergeTree ORDER BY a
    AND INSERT SELECT number AS a, toString(number) AS b FROM numbers(3);
SELECT a, b FROM t5 ORDER BY a;
DROP TABLE t5;

DROP TABLE IF EXISTS t6;
CREATE TABLE t6 (a UInt32, b String) ENGINE = MergeTree ORDER BY a;
INSERT INTO t6 VALUES (10, 'existing');
CREATE TABLE IF NOT EXISTS t6 (a UInt32, b String) ENGINE = MergeTree ORDER BY a
    AND INSERT SELECT number AS a, toString(number) AS b FROM numbers(2);
SELECT a, b FROM t6 ORDER BY a;
DROP TABLE t6;

DROP TABLE IF EXISTS t7;
CREATE TABLE IF NOT EXISTS t7 (a UInt32, b String) ENGINE = MergeTree ORDER BY a
    AND INSERT VALUES (1, 'a'), (2, 'b');
SELECT a, b FROM t7 ORDER BY a;

CREATE TABLE IF NOT EXISTS t7 (a UInt32, b String) ENGINE = MergeTree ORDER BY a
    AND INSERT VALUES (3, 'c');
SELECT a, b FROM t7 ORDER BY a;
DROP TABLE t7;

CREATE TABLE t9 ENGINE = MergeTree ORDER BY a
    AS INSERT VALUES (1, 'x'); -- { serverError INCORRECT_QUERY }

DROP TABLE IF EXISTS t10;
CREATE TABLE t10 (a UInt32, b String) ENGINE = MergeTree ORDER BY a
    AS INSERT FORMAT Values (100, 'fmt_test');

SELECT a, b FROM t10 ORDER BY a;
DROP TABLE t10;

SET format_csv_delimiter = ';';

DROP TABLE IF EXISTS t_fmt1;
CREATE TABLE t_fmt1 (a UInt32, b String) ENGINE = MergeTree ORDER BY a
    AS INSERT FORMAT JSONEachRow {"a":1,"b":"alpha"}
{"a":2,"b":"beta"};

SELECT a, b FROM t_fmt1 ORDER BY a;
DROP TABLE t_fmt1;


DROP TABLE IF EXISTS t_fmt2;
CREATE TABLE IF NOT EXISTS t_fmt2 (a UInt32, b String) ENGINE = MergeTree ORDER BY a
    AND INSERT FORMAT CSV 1;"csv_row1"
2;"csv_row2";

SELECT a, b FROM t_fmt2 ORDER BY a;
DROP TABLE t_fmt2;


DROP TABLE IF EXISTS t_fmt3;
CREATE TABLE t_fmt3 (a UInt32, b String) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_fmt3 VALUES (10, 'existing');

CREATE TABLE IF NOT EXISTS t_fmt3 (a UInt32, b String) ENGINE = MergeTree ORDER BY a
    AND INSERT FORMAT JSONEachRow {"a":1,"b":"new_row"};

SELECT a, b FROM t_fmt3 ORDER BY a;
DROP TABLE t_fmt3;

DROP TABLE IF EXISTS t_input1;
CREATE TABLE t_input1 (a UInt32, b String) ENGINE = MergeTree ORDER BY a
    AS INSERT SELECT * FROM input('a UInt32, b String') FORMAT CSV
1;"input_row1"
2;"input_row2";

SELECT a, b FROM t_input1 ORDER BY a;
DROP TABLE t_input1;

DROP TABLE IF EXISTS t_input2;
CREATE TABLE t_input2 (a UInt32, b String) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_input2 VALUES (10, 'existing');

CREATE TABLE IF NOT EXISTS t_input2 (a UInt32, b String) ENGINE = MergeTree ORDER BY a
    AND INSERT SELECT * FROM input('a UInt32, b String') FORMAT CSV
1;"new_via_input";

SELECT a, b FROM t_input2 ORDER BY a;
DROP TABLE t_input2;

-- Test 1: `parseQueryToJSON` / `formatQueryFromJSON` keep the `AS INSERT` clause.
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE IF NOT EXISTS t (a UInt8) ENGINE = Memory AS INSERT SELECT 1'));

-- Test 2: inline data cannot be represented in the JSON AST, so it is rejected instead of being silently dropped.
SELECT parseQueryToJSON('CREATE TABLE t (a UInt8) ENGINE = Memory AS INSERT VALUES (1), (2)'); -- { serverError BAD_ARGUMENTS }

DROP TABLE IF EXISTS t_raw_settings;
CREATE TABLE t_raw_settings (x String) ENGINE = Memory AS INSERT FORMAT CSV
SETTINGS made_up_setting = 1

CREATE TABLE IF NOT EXISTS t_raw_settings (x String) ENGINE = Memory AND INSERT FORMAT CSV
SETTINGS made_up_setting = 1

INSERT INTO t_raw_settings FORMAT CSV
SETTINGS made_up_setting = 1

SELECT x, count() FROM t_raw_settings GROUP BY x;
DROP TABLE t_raw_settings;

DROP TABLE IF EXISTS t_raw_input;
CREATE TABLE t_raw_input (x String) ENGINE = Memory AS INSERT SELECT * FROM input('x String') FORMAT CSV
SETTINGS made_up_setting = 1

SELECT x FROM t_raw_input;
DROP TABLE t_raw_input;

SELECT formatQuerySingleLine(concat('CREATE TABLE t (x String) ENGINE = Memory AS INSERT FORMAT CSV', '\nCOMMENT ''payload'''))
    = formatQuerySingleLine('CREATE TABLE t (x String) ENGINE = Memory AS INSERT FORMAT CSV');
SELECT formatQuerySingleLine(concat('CREATE TABLE t (x String) ENGINE = Memory AS INSERT FORMAT CSV', '\nPARALLEL WITH SELECT 1'))
    = formatQuerySingleLine('CREATE TABLE t (x String) ENGINE = Memory AS INSERT FORMAT CSV');

SELECT 1 AND insert FROM (SELECT 1 AS insert);

DROP TABLE IF EXISTS t_noop_followed_by_insert;
CREATE TABLE t_noop_followed_by_insert (x UInt8) ENGINE = Memory;
CREATE TABLE IF NOT EXISTS t_noop_followed_by_insert AS INSERT VALUES (1) -- comment after data
;
INSERT INTO t_noop_followed_by_insert VALUES (2);
SELECT x FROM t_noop_followed_by_insert;
DROP TABLE t_noop_followed_by_insert;

DROP TABLE IF EXISTS t_population_metadata;
CREATE TABLE t_population_metadata (x UInt64) ENGINE = MergeTree ORDER BY x
    AS INSERT SELECT number FROM numbers(3) SETTINGS limit = 1;
CREATE TABLE IF NOT EXISTS t_population_metadata (x UInt64) ENGINE = MergeTree ORDER BY x
    AND INSERT SELECT number + 10 FROM numbers(3) SETTINGS limit = 1;
SELECT count(), sum(x) FROM t_population_metadata;
SELECT positionCaseInsensitive(create_table_query, ' AS INSERT ') = 0
    AND positionCaseInsensitive(create_table_query, ' AND INSERT ') = 0
    FROM system.tables WHERE database = currentDatabase() AND name = 't_population_metadata';
DETACH TABLE t_population_metadata SYNC;
ATTACH TABLE t_population_metadata;
SELECT count(), sum(x) FROM t_population_metadata;
DROP TABLE t_population_metadata;

CREATE TEMPORARY TABLE IF NOT EXISTS t_temporary_population (x UInt8) AND INSERT VALUES (1);
CREATE TEMPORARY TABLE IF NOT EXISTS t_temporary_population AND INSERT VALUES (2);
SELECT count(), sum(x) FROM t_temporary_population;
DROP TEMPORARY TABLE t_temporary_population;