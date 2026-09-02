-- The result type of `IN` over a full `LowCardinality` column must not depend on the form of the
-- right-hand side, and must be the same during analysis and during execution. The old analyzer
-- replaces `in` with `inIgnoreSet` while it only needs the types, and used to pass the left operand
-- as the stand-in for the set that is not built yet: two full `LowCardinality` arguments type as
-- plain `UInt8`, while executing the real `in` against a constant set yields `LowCardinality(UInt8)`.
-- Reading such a column across a subquery boundary then failed the type check in
-- `ActionsDAG::updateHeader` with `Unexpected return type from tuple` (`LOGICAL_ERROR`).
-- The `arrayFilter` queries cover the same rewrite inside a lambda, whose captured arguments
-- must not gain columns that later analysis passes never create.

DROP TABLE IF EXISTS t_in_lc_type;
CREATE TABLE t_in_lc_type (s LowCardinality(String)) ENGINE = MergeTree ORDER BY s;
INSERT INTO t_in_lc_type VALUES ('a'), ('b');

SELECT '-- old analyzer';
SET enable_analyzer = 0;

SELECT DISTINCT toTypeName(s IN ('a')) AS literal_set, toTypeName(s IN (SELECT 'a')) AS subquery_set FROM t_in_lc_type;
SELECT DISTINCT toTypeName(f) FROM (SELECT s IN (SELECT 'a') AS f FROM t_in_lc_type);
SELECT tuple(*) AS t FROM (SELECT s, s IN (SELECT 'a') AS f FROM t_in_lc_type) ORDER BY t;
SELECT arrayFilter(x -> (x IN (SELECT 'a')), [s, 'b']) FROM t_in_lc_type ORDER BY s;
SELECT count() FROM t_in_lc_type WHERE s IN (SELECT 'a');
SELECT count() FROM t_in_lc_type WHERE s NOT IN (SELECT 'a');

SELECT '-- the analyzer';
SET enable_analyzer = 1;

SELECT DISTINCT toTypeName(s IN ('a')) AS literal_set, toTypeName(s IN (SELECT 'a')) AS subquery_set FROM t_in_lc_type;
SELECT DISTINCT toTypeName(f) FROM (SELECT s IN (SELECT 'a') AS f FROM t_in_lc_type);
SELECT tuple(*) AS t FROM (SELECT s, s IN (SELECT 'a') AS f FROM t_in_lc_type) ORDER BY t;
SELECT arrayFilter(x -> (x IN (SELECT 'a')), [s, 'b']) FROM t_in_lc_type ORDER BY s;
SELECT count() FROM t_in_lc_type WHERE s IN (SELECT 'a');
SELECT count() FROM t_in_lc_type WHERE s NOT IN (SELECT 'a');

DROP TABLE t_in_lc_type;
