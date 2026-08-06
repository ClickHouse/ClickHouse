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
DROP TABLE IF EXISTS t04741_lc_dst;
DROP TABLE IF EXISTS t04741_lc_buf;
DROP TABLE IF EXISTS t04741_bare_dst;
DROP TABLE IF EXISTS t04741_bare_buf;
DROP TABLE IF EXISTS t04741_nul_dst;
DROP TABLE IF EXISTS t04741_nul_buf;
DROP ROW POLICY IF EXISTS p04741_map ON t04741_map_buf;
DROP ROW POLICY IF EXISTS p04741_map_k ON t04741_map_buf;
DROP ROW POLICY IF EXISTS p04741_arr ON t04741_arr_buf;
DROP ROW POLICY IF EXISTS p04741_same ON t04741_same_buf;
DROP ROW POLICY IF EXISTS p04741_wrap ON t04741_wrap_buf;
DROP ROW POLICY IF EXISTS p04741_lc ON t04741_lc_buf;
DROP ROW POLICY IF EXISTS p04741_bare ON t04741_bare_buf;
DROP ROW POLICY IF EXISTS p04741_nul ON t04741_nul_buf;

-- Arms A, F, W, W2, Z, Y and D each hold a row both filters accept, a row only its row policy
-- rejects and a row only its PREWHERE rejects, so neither filter can go missing unnoticed. J and I
-- are the deliberate exceptions, each showing a filter on another column is unaffected: J has no
-- policy-only-rejected row and I has no PREWHERE-only-rejected row. N pairs the PREWHERE with an
-- additional_table_filters entry instead of a row policy, and B, G and C are single-filter controls.
CREATE TABLE t04741_map_dst (k UInt8, m Array(Tuple(String, UInt64))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t04741_map_dst VALUES (1, [('a', 1), ('b', 2)]), (2, [('b', 2)]), (3, [('a', 1)]);
CREATE TABLE t04741_map_buf (k UInt8, m Map(String, UInt64))
    ENGINE = Buffer(currentDatabase(), t04741_map_dst, 1, 100, 100, 1000, 10000, 1000000, 10000000);

CREATE TABLE t04741_arr_dst (k UInt8, a Array(UInt64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t04741_arr_dst VALUES (1, [10, 20, 30]), (2, []), (3, [40]), (4, [50, 60]);
CREATE TABLE t04741_arr_buf (k UInt8, a Array(String))
    ENGINE = Buffer(currentDatabase(), t04741_arr_dst, 1, 100, 100, 1000, 10000, 1000000, 10000000);

CREATE TABLE t04741_wrap_dst (k UInt8, s Nullable(UInt64), l String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t04741_wrap_dst VALUES (1, 7, 'x'), (2, NULL, 'q'), (3, 9, '');
CREATE TABLE t04741_wrap_buf (k UInt8, s Nullable(String), l LowCardinality(String))
    ENGINE = Buffer(currentDatabase(), t04741_wrap_dst, 1, 100, 100, 1000, 10000, 1000000, 10000000);

CREATE TABLE t04741_lc_dst (k UInt8, l String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t04741_lc_dst VALUES (1, 'y'), (2, ''), (3, 'zz');
CREATE TABLE t04741_lc_buf (k UInt8, l LowCardinality(String))
    ENGINE = Buffer(currentDatabase(), t04741_lc_dst, 1, 100, 100, 1000, 10000, 1000000, 10000000);

CREATE TABLE t04741_bare_dst (k UInt8, f UInt8) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t04741_bare_dst VALUES (1, 5), (2, 0), (3, 1), (4, 2);
CREATE TABLE t04741_bare_buf (k UInt8, f UInt64)
    ENGINE = Buffer(currentDatabase(), t04741_bare_dst, 1, 100, 100, 1000, 10000, 1000000, 10000000);

CREATE TABLE t04741_nul_dst (k UInt8, n Nullable(UInt64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t04741_nul_dst VALUES (1, 7), (2, NULL), (3, 9), (4, 8);
CREATE TABLE t04741_nul_buf (k UInt8, n Nullable(String))
    ENGINE = Buffer(currentDatabase(), t04741_nul_dst, 1, 100, 100, 1000, 10000, 1000000, 10000000);

CREATE TABLE t04741_same_dst (k UInt8, m Map(String, UInt64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t04741_same_dst VALUES (1, map('a', 1, 'b', 2)), (2, map('b', 2)), (3, map('a', 1));
CREATE TABLE t04741_same_buf (k UInt8, m Map(String, UInt64))
    ENGINE = Buffer(currentDatabase(), t04741_same_dst, 1, 100, 100, 1000, 10000, 1000000, 10000000);

-- The single-filter controls read the Buffer before any row policy exists on their table.
SELECT 'B single PREWHERE on the converted column';
SELECT m FROM t04741_map_buf PREWHERE mapContains(m, 'b') ORDER BY m;

SELECT 'G Array parent, single PREWHERE';
SELECT a FROM t04741_arr_buf PREWHERE length(a) > 1 ORDER BY a;

CREATE ROW POLICY p04741_map ON t04741_map_buf USING mapContains(m, 'a') TO ALL;

SELECT 'C single row policy on the converted column';
SELECT m FROM t04741_map_buf ORDER BY m;

-- Both forwarded filters reference the same converted column: the row policy leaves it converted
-- and the PREWHERE prefix must not convert it again.
SELECT 'A row policy and PREWHERE on the converted column';
SELECT m FROM t04741_map_buf PREWHERE mapContains(m, 'b') ORDER BY m;

SELECT 'J PREWHERE on another column';
SELECT m FROM t04741_map_buf PREWHERE k = 1 ORDER BY m;

DROP ROW POLICY p04741_map ON t04741_map_buf;
CREATE ROW POLICY p04741_map_k ON t04741_map_buf USING k = 1 TO ALL;

SELECT 'I row policy on another column';
SELECT m FROM t04741_map_buf PREWHERE mapContains(m, 'b') ORDER BY m;

DROP ROW POLICY p04741_map_k ON t04741_map_buf;

SELECT 'N additional_table_filters and PREWHERE';
SELECT m FROM t04741_map_buf PREWHERE mapContains(m, 'b') ORDER BY m
SETTINGS additional_table_filters = {'t04741_map_buf': 'mapContains(m, \'a\')'};

-- A different parent type reaches a different cast, so cover it too. The policy needs a conjunct the
-- PREWHERE does not imply, or a row rejected only by the policy cannot exist.
CREATE ROW POLICY p04741_arr ON t04741_arr_buf USING length(a) > 0 AND k != 4 TO ALL;

SELECT 'F Array parent, row policy and PREWHERE';
SELECT a FROM t04741_arr_buf PREWHERE length(a) > 1 ORDER BY a;

-- Nullable and LowCardinality parents reach further distinct casts.
CREATE ROW POLICY p04741_wrap ON t04741_wrap_buf USING s IS NOT NULL TO ALL;

SELECT 'W Nullable parent, filters on distinct columns';
SELECT s, l FROM t04741_wrap_buf PREWHERE l != '' ORDER BY s;

CREATE ROW POLICY p04741_lc ON t04741_lc_buf USING l != '' TO ALL;

SELECT 'W2 LowCardinality parent, both filters on the same column';
SELECT l FROM t04741_lc_buf PREWHERE length(l) < 2 ORDER BY l;

-- A bare-column row policy makes the filter column a real table column, so the row-level step
-- removes a column it also emits converted.
CREATE ROW POLICY p04741_bare ON t04741_bare_buf USING f TO ALL;

SELECT 'Z bare-column row policy and PREWHERE on the same column';
SELECT f FROM t04741_bare_buf PREWHERE f < 4 ORDER BY f;

-- A Nullable parent with both filters on the same column. This is a control: a Nullable parent was
-- already correct before the fix, so the arm pins that the fix does not regress it. Both filters
-- reject the NULL row, so the arm's sensitivity comes from the other two rows: n = '9' only the
-- policy rejects, n = '7' only the PREWHERE rejects.
CREATE ROW POLICY p04741_nul ON t04741_nul_buf USING n != '9' TO ALL;

SELECT 'Y Nullable parent, both filters on the same column';
SELECT n FROM t04741_nul_buf PREWHERE n != '7' ORDER BY n;

CREATE ROW POLICY p04741_same ON t04741_same_buf USING mapContains(m, 'a') TO ALL;

SELECT 'D matching types, row policy and PREWHERE';
SELECT m FROM t04741_same_buf PREWHERE mapContains(m, 'b') ORDER BY m;

DROP ROW POLICY p04741_arr ON t04741_arr_buf;
DROP ROW POLICY p04741_same ON t04741_same_buf;
DROP ROW POLICY p04741_wrap ON t04741_wrap_buf;
DROP ROW POLICY p04741_lc ON t04741_lc_buf;
DROP ROW POLICY p04741_bare ON t04741_bare_buf;
DROP ROW POLICY p04741_nul ON t04741_nul_buf;
DROP TABLE t04741_map_buf;
DROP TABLE t04741_map_dst;
DROP TABLE t04741_arr_buf;
DROP TABLE t04741_arr_dst;
DROP TABLE t04741_same_buf;
DROP TABLE t04741_same_dst;
DROP TABLE t04741_wrap_buf;
DROP TABLE t04741_wrap_dst;
DROP TABLE t04741_lc_buf;
DROP TABLE t04741_lc_dst;
DROP TABLE t04741_bare_buf;
DROP TABLE t04741_bare_dst;
DROP TABLE t04741_nul_buf;
DROP TABLE t04741_nul_dst;
