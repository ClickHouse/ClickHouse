-- A LowCardinality(Float) dictionary is not canonicalized: depending on the insert path, -0.0 can end up as
-- a dictionary entry of its own next to +0.0, or be folded onto the +0.0 entry. Yet -0.0 compares equal to
-- +0.0, so the two variants of DISTINCT and LIMIT BY would disagree with each other if the in-order variant
-- grouped such a key by comparison: it would merge the two zeros into a single group, while the hash variant
-- and GROUP BY keep them apart. A LowCardinality float key therefore stops the sort prefix, and DISTINCT and
-- LIMIT BY group it by hash, in agreement with GROUP BY.
--
-- These queries assert that agreement. Whether -0.0 survives into the dictionary as a separate entry depends
-- on the insert path (and, for inline VALUES, on which side of the connection parses them), so the group
-- counts themselves are not stable across settings; the equality of the two counts is. The reference for
-- every comparison is GROUP BY with in-order aggregation turned off, which always groups by hash.

SET allow_suspicious_low_cardinality_types = 1;

-- LIMIT BY over a sorted stream, Float64: the zero rows form as many groups as GROUP BY sees.
DROP TABLE IF EXISTS lcv;
CREATE TABLE lcv (x LowCardinality(Float64)) ENGINE = Memory;
INSERT INTO lcv SELECT 0.0 FROM numbers(100);
INSERT INTO lcv VALUES (-0.0);
INSERT INTO lcv SELECT 0.0 FROM numbers(100);
SELECT (SELECT count() FROM (SELECT x FROM lcv ORDER BY x LIMIT 1 BY x SETTINGS max_threads = 1))
     = (SELECT count() FROM (SELECT x FROM lcv GROUP BY x SETTINGS optimize_aggregation_in_order = 0));
DROP TABLE lcv;

-- LIMIT BY and negative LIMIT BY over a sorted stream, Float32, with a second key value 1.0.
DROP TABLE IF EXISTS lcv32;
CREATE TABLE lcv32 (x LowCardinality(Float32)) ENGINE = Memory;
INSERT INTO lcv32 VALUES (0.0)(-0.0)(1.0)(-0.0)(0.0);
SELECT (SELECT count() FROM (SELECT x FROM lcv32 ORDER BY x LIMIT 1 BY x SETTINGS max_threads = 1))
     = (SELECT count() FROM (SELECT x FROM lcv32 GROUP BY x SETTINGS optimize_aggregation_in_order = 0));
SELECT (SELECT count() FROM (SELECT x FROM lcv32 ORDER BY x LIMIT -1 BY x SETTINGS max_threads = 1))
     = (SELECT count() FROM (SELECT x FROM lcv32 GROUP BY x SETTINGS optimize_aggregation_in_order = 0));
DROP TABLE lcv32;

-- Nullable inner type, exercising the unwrap that looks through Nullable to the float inner type:
-- NULL is one more group in both variants.
DROP TABLE IF EXISTS lcn;
CREATE TABLE lcn (x LowCardinality(Nullable(Float64))) ENGINE = Memory;
INSERT INTO lcn VALUES (0.0)(-0.0)(NULL)(0.0)(-0.0)(NULL);
SELECT (SELECT count() FROM (SELECT x FROM lcn ORDER BY x LIMIT 1 BY x SETTINGS max_threads = 1))
     = (SELECT count() FROM (SELECT x FROM lcn GROUP BY x SETTINGS optimize_aggregation_in_order = 0));
DROP TABLE lcn;

-- MergeTree sorted by the key, exercising the dictionary deserialized from a part together with reading in
-- primary-key order: DISTINCT and LIMIT BY still agree with GROUP BY, whatever the dictionary holds.
DROP TABLE IF EXISTS lc_mt;
CREATE TABLE lc_mt (x LowCardinality(Float64)) ENGINE = MergeTree ORDER BY x;
INSERT INTO lc_mt SELECT 0.0 FROM numbers(50);
INSERT INTO lc_mt VALUES (-0.0);
INSERT INTO lc_mt SELECT 0.0 FROM numbers(50);
OPTIMIZE TABLE lc_mt FINAL;
SELECT (SELECT count() FROM (SELECT DISTINCT x FROM lc_mt SETTINGS max_threads = 1, optimize_distinct_in_order = 1))
     = (SELECT count() FROM (SELECT x FROM lc_mt GROUP BY x SETTINGS optimize_aggregation_in_order = 0));
SELECT (SELECT count() FROM (SELECT x FROM lc_mt ORDER BY x LIMIT 1 BY x SETTINGS max_threads = 1))
     = (SELECT count() FROM (SELECT x FROM lc_mt GROUP BY x SETTINGS optimize_aggregation_in_order = 0));
DROP TABLE lc_mt;
