-- A LowCardinality(Float) dictionary can hold -0.0 and +0.0 as two distinct entries: it is not
-- canonicalized, neither on insert nor when deserialized from storage. Yet -0.0 compares equal to +0.0,
-- so the sorted-stream operators (DISTINCT / LIMIT BY in order) must group by value, not by dictionary
-- index, and collapse them into a single group. These queries assert the resulting group counts. The
-- counts are stable, while which of the two equal representatives is emitted is not, as it depends on the
-- ordering of equal-comparing values.

SET allow_suspicious_low_cardinality_types = 1;

-- LIMIT BY in order, Float64: all rows share one value-equal key, so LIMIT 1 BY yields a single group.
DROP TABLE IF EXISTS lcv;
CREATE TABLE lcv (x LowCardinality(Float64)) ENGINE = Memory;
INSERT INTO lcv SELECT 0.0 FROM numbers(100);
INSERT INTO lcv VALUES (-0.0);
INSERT INTO lcv SELECT 0.0 FROM numbers(100);
SELECT count() FROM (SELECT x FROM lcv ORDER BY x LIMIT 1 BY x SETTINGS max_threads = 1);
DROP TABLE lcv;

-- LIMIT BY and negative LIMIT BY in order, Float32: the zero values form one group, 1.0 another.
DROP TABLE IF EXISTS lcv32;
CREATE TABLE lcv32 (x LowCardinality(Float32)) ENGINE = Memory;
INSERT INTO lcv32 VALUES (0.0)(-0.0)(1.0)(-0.0)(0.0);
SELECT count() FROM (SELECT x FROM lcv32 ORDER BY x LIMIT 1 BY x SETTINGS max_threads = 1);
SELECT count() FROM (SELECT x FROM lcv32 ORDER BY x LIMIT -1 BY x SETTINGS max_threads = 1);
DROP TABLE lcv32;

-- Nullable inner type, exercising the unwrap that looks through Nullable to the float inner type:
-- the zero values form one group and NULL another.
DROP TABLE IF EXISTS lcn;
CREATE TABLE lcn (x LowCardinality(Nullable(Float64))) ENGINE = Memory;
INSERT INTO lcn VALUES (0.0)(-0.0)(NULL)(0.0)(-0.0)(NULL);
SELECT count() FROM (SELECT x FROM lcn ORDER BY x LIMIT 1 BY x SETTINGS max_threads = 1);
DROP TABLE lcn;

-- MergeTree, exercising the deserialized-dictionary path together with reading in primary-key order:
-- the dictionary read back from parts is not canonicalized, so -0.0 and +0.0 remain separate entries and
-- value comparison must still collapse them in both DISTINCT in order and LIMIT BY in order.
DROP TABLE IF EXISTS lc_mt;
CREATE TABLE lc_mt (x LowCardinality(Float64)) ENGINE = MergeTree ORDER BY x;
INSERT INTO lc_mt SELECT 0.0 FROM numbers(50);
INSERT INTO lc_mt VALUES (-0.0);
INSERT INTO lc_mt SELECT 0.0 FROM numbers(50);
OPTIMIZE TABLE lc_mt FINAL;
SELECT count() FROM (SELECT DISTINCT x FROM lc_mt SETTINGS max_threads = 1, optimize_distinct_in_order = 1);
SELECT count() FROM (SELECT x FROM lc_mt ORDER BY x LIMIT 1 BY x SETTINGS max_threads = 1);
DROP TABLE lc_mt;
