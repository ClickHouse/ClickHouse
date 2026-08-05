-- Tags: no-parallel-replicas

-- Reading a Buffer whose destination declares a column differently logs a warning per read.
SET send_logs_level = 'error';

DROP TABLE IF EXISTS t04741_map_dst;
DROP TABLE IF EXISTS t04741_map_buf;
DROP TABLE IF EXISTS t04741_arr_dst;
DROP TABLE IF EXISTS t04741_arr_buf;
DROP TABLE IF EXISTS t04741_same_dst;
DROP TABLE IF EXISTS t04741_same_buf;
DROP TABLE IF EXISTS t04741_wrap_dst;
DROP TABLE IF EXISTS t04741_wrap_buf;
DROP ROW POLICY IF EXISTS p04741_map ON t04741_map_buf;
DROP ROW POLICY IF EXISTS p04741_map_k ON t04741_map_buf;
DROP ROW POLICY IF EXISTS p04741_arr ON t04741_arr_buf;
DROP ROW POLICY IF EXISTS p04741_same ON t04741_same_buf;
DROP ROW POLICY IF EXISTS p04741_wrap ON t04741_wrap_buf;

CREATE TABLE t04741_map_dst (k UInt8, m Array(Tuple(String, UInt64))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t04741_map_dst VALUES (1, [('a', 1), ('b', 2)]);
CREATE TABLE t04741_map_buf (k UInt8, m Map(String, UInt64))
    ENGINE = Buffer(currentDatabase(), t04741_map_dst, 1, 100, 100, 1000, 10000, 1000000, 10000000);

CREATE TABLE t04741_arr_dst (k UInt8, a Array(UInt64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t04741_arr_dst VALUES (1, [10, 20, 30]);
CREATE TABLE t04741_arr_buf (k UInt8, a Array(String))
    ENGINE = Buffer(currentDatabase(), t04741_arr_dst, 1, 100, 100, 1000, 10000, 1000000, 10000000);

CREATE TABLE t04741_wrap_dst (k UInt8, s Nullable(UInt64), l String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t04741_wrap_dst VALUES (1, 7, 'x');
CREATE TABLE t04741_wrap_buf (k UInt8, s Nullable(String), l LowCardinality(String))
    ENGINE = Buffer(currentDatabase(), t04741_wrap_dst, 1, 100, 100, 1000, 10000, 1000000, 10000000);

CREATE TABLE t04741_same_dst (k UInt8, m Map(String, UInt64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t04741_same_dst VALUES (1, map('a', 1, 'b', 2));
CREATE TABLE t04741_same_buf (k UInt8, m Map(String, UInt64))
    ENGINE = Buffer(currentDatabase(), t04741_same_dst, 1, 100, 100, 1000, 10000, 1000000, 10000000);

SELECT 'B single PREWHERE on the converted column';
SELECT m FROM t04741_map_buf PREWHERE mapContains(m, 'b');

CREATE ROW POLICY p04741_map ON t04741_map_buf USING mapContains(m, 'a') TO ALL;

SELECT 'C single row policy on the converted column';
SELECT m FROM t04741_map_buf;

-- Both forwarded filters reference the same converted column: the row policy leaves it converted
-- and the PREWHERE prefix must not convert it again.
SELECT 'A row policy and PREWHERE on the converted column';
SELECT m FROM t04741_map_buf PREWHERE mapContains(m, 'b');

SELECT 'J PREWHERE on another column';
SELECT m FROM t04741_map_buf PREWHERE k = 1;

DROP ROW POLICY p04741_map ON t04741_map_buf;
CREATE ROW POLICY p04741_map_k ON t04741_map_buf USING k = 1 TO ALL;

SELECT 'I row policy on another column';
SELECT m FROM t04741_map_buf PREWHERE mapContains(m, 'b');

DROP ROW POLICY p04741_map_k ON t04741_map_buf;

SELECT 'N additional_table_filters and PREWHERE';
SELECT m FROM t04741_map_buf PREWHERE mapContains(m, 'b')
SETTINGS additional_table_filters = {'t04741_map_buf': 'mapContains(m, \'a\')'};

-- A different parent type reaches a different cast, so cover it too.
CREATE ROW POLICY p04741_arr ON t04741_arr_buf USING length(a) > 0 TO ALL;

SELECT 'F Array parent, row policy and PREWHERE';
SELECT a FROM t04741_arr_buf PREWHERE length(a) > 1;

SELECT 'G Array parent, single PREWHERE';
SELECT a FROM t04741_arr_dst PREWHERE length(a) > 1;

-- Nullable and LowCardinality parents reach further distinct casts.
CREATE ROW POLICY p04741_wrap ON t04741_wrap_buf USING s IS NOT NULL TO ALL;

SELECT 'W Nullable and LowCardinality parents, row policy and PREWHERE';
SELECT s, l FROM t04741_wrap_buf PREWHERE l != '';

CREATE ROW POLICY p04741_same ON t04741_same_buf USING mapContains(m, 'a') TO ALL;

SELECT 'D matching types, row policy and PREWHERE';
SELECT m FROM t04741_same_buf PREWHERE mapContains(m, 'b');

DROP ROW POLICY p04741_arr ON t04741_arr_buf;
DROP ROW POLICY p04741_same ON t04741_same_buf;
DROP ROW POLICY p04741_wrap ON t04741_wrap_buf;
DROP TABLE t04741_map_buf;
DROP TABLE t04741_map_dst;
DROP TABLE t04741_arr_buf;
DROP TABLE t04741_arr_dst;
DROP TABLE t04741_same_buf;
DROP TABLE t04741_same_dst;
DROP TABLE t04741_wrap_buf;
DROP TABLE t04741_wrap_dst;
