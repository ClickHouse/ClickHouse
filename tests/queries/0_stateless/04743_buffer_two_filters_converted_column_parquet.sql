-- Tags: no-fasttest
-- no-fasttest: the Parquet format is unavailable in the fast test build.

-- Reading a Buffer whose destination declares a column differently logs a warning per read.
SET send_logs_level = 'fatal';

DROP TABLE IF EXISTS t04743_pq_buf;
DROP TABLE IF EXISTS t04743_pq_dst;
DROP TABLE IF EXISTS t04743_bare_buf;
DROP TABLE IF EXISTS t04743_bare_dst;
DROP ROW POLICY IF EXISTS p04743_pq ON t04743_pq_buf;

-- A Parquet-backed destination forwards both filters through StorageFile, which builds their header
-- in updateFormatPrewhereInfo. MergeTree never reaches that function, so the arm below covers a
-- carrier the sibling test cannot.
CREATE TABLE t04743_pq_dst (k UInt8, m Array(Tuple(String, UInt64))) ENGINE = File(Parquet);
INSERT INTO t04743_pq_dst VALUES (1, [('a', 1), ('b', 2)]), (2, [('b', 2)]), (3, [('a', 1)]);
CREATE TABLE t04743_pq_buf (k UInt8, m Map(String, UInt64))
    ENGINE = Buffer(currentDatabase(), t04743_pq_dst, 1, 100, 100, 1000, 10000, 1000000, 10000000);

-- A bare PREWHERE makes the predicate and the carried column the same node, so the converting
-- prefix has to separate the two roles. Float64 into UInt8 changes truthiness rather than
-- preserving it, so a predicate left on the destination's own column returns a row too many.
CREATE TABLE t04743_bare_dst (k UInt8, f Float64) ENGINE = File(Parquet);
INSERT INTO t04743_bare_dst VALUES (1, 0.5), (2, 0.0), (3, 2.0);
CREATE TABLE t04743_bare_buf (k UInt8, f UInt8)
    ENGINE = Buffer(currentDatabase(), t04743_bare_dst, 1, 100, 100, 1000, 10000, 1000000, 10000000);

SELECT 'P1 no filter, the converted column reads correctly';
SELECT m FROM t04743_pq_buf ORDER BY k;

SELECT 'P2 single row policy on the converted column';
CREATE ROW POLICY p04743_pq ON t04743_pq_buf USING mapContains(m, 'a') TO ALL;
SELECT m FROM t04743_pq_buf ORDER BY k;

-- The rows above are the same whether the policy is forwarded into the destination read or applied
-- above it, and only the forwarded path runs the converting prefix this test covers. The marker
-- comes from query_info.row_level_filter, which both planners populate; pretty = 0 makes it
-- unconditional.
SELECT 'P2 the policy reached the destination read';
SELECT count() > 0 FROM (EXPLAIN actions = 1, pretty = 0 SELECT m FROM t04743_pq_buf ORDER BY k)
WHERE explain LIKE '%Row level filter column:%';

SET enable_analyzer = 0;
SELECT 'P2 the same with the legacy analyzer';
SELECT count() > 0 FROM (EXPLAIN actions = 1, pretty = 0 SELECT m FROM t04743_pq_buf ORDER BY k)
WHERE explain LIKE '%Row level filter column:%';
SET enable_analyzer = 1;

-- Both forwarded filters reference the same converted column. The row policy prefix leaves it
-- holding this table's type, so the PREWHERE prefix must not convert it a second time.
SELECT 'P3 row policy and PREWHERE on the converted column';
SELECT m FROM t04743_pq_buf PREWHERE mapContains(m, 'b') ORDER BY k;

SELECT 'P3 the policy reached the destination read';
SELECT count() > 0 FROM (EXPLAIN actions = 1, pretty = 0 SELECT m FROM t04743_pq_buf PREWHERE mapContains(m, 'b') ORDER BY k)
WHERE explain LIKE '%Row level filter column:%';

SET enable_analyzer = 0;
SELECT 'P3 the same with the legacy analyzer';
SELECT count() > 0 FROM (EXPLAIN actions = 1, pretty = 0 SELECT m FROM t04743_pq_buf PREWHERE mapContains(m, 'b') ORDER BY k)
WHERE explain LIKE '%Row level filter column:%';
SET enable_analyzer = 1;

-- A bare PREWHERE is the only shape where the predicate is itself the carried column, and it
-- needs no setting to reach the destination read. The row this admits tells the two apart: only
-- a converted predicate rejects 0.5. The internal alias must also stay out of that read's header.
SELECT 'P4 bare PREWHERE on the converted column';
SELECT f FROM t04743_bare_buf PREWHERE f ORDER BY f;

SELECT 'P4 the alias stays out of the read header';
SELECT count() FROM (EXPLAIN header = 1, pretty = 0 SELECT * FROM t04743_bare_buf PREWHERE f ORDER BY f)
WHERE explain LIKE '%Header:%' AND explain LIKE '%__buffer_converted_filter%';

SET enable_analyzer = 0;
SELECT 'P4 the same with the legacy analyzer';
SELECT f FROM t04743_bare_buf PREWHERE f ORDER BY f;
SELECT count() FROM (EXPLAIN header = 1, pretty = 0 SELECT * FROM t04743_bare_buf PREWHERE f ORDER BY f)
WHERE explain LIKE '%Header:%' AND explain LIKE '%__buffer_converted_filter%';
SET enable_analyzer = 1;

DROP ROW POLICY p04743_pq ON t04743_pq_buf;
DROP TABLE t04743_pq_buf;
DROP TABLE t04743_pq_dst;
DROP TABLE t04743_bare_buf;
DROP TABLE t04743_bare_dst;
