-- A filter deferred until after FINAL must keep its own predicate column in the stream when that
-- predicate is a bare column reference. Every arm prints a value, not just "did not throw".

DROP TABLE IF EXISTS t_pw;
CREATE TABLE t_pw (k UInt64, b UInt8, other UInt8, n Nullable(UInt8)) ENGINE = ReplacingMergeTree() ORDER BY k;
INSERT INTO t_pw SELECT number, number % 2, number % 3, if(number % 4 = 0, NULL, number) FROM numbers(20);

-- Ground truth, no deferral involved.
SELECT 'gt where b', count() FROM t_pw FINAL WHERE b;
SELECT 'gt where n.null', count() FROM t_pw FINAL WHERE n.null;

-- Deferred PREWHERE on a bare column, across projections.
SELECT 'pw count', count() FROM t_pw FINAL PREWHERE b SETTINGS apply_prewhere_after_final = 1;
SELECT 'pw select k', sum(k) FROM (SELECT k FROM t_pw FINAL PREWHERE b SETTINGS apply_prewhere_after_final = 1);
SELECT 'pw select b', count() FROM (SELECT b FROM t_pw FINAL PREWHERE b SETTINGS apply_prewhere_after_final = 1);
SELECT 'pw select star', count() FROM (SELECT * FROM t_pw FINAL PREWHERE b SETTINGS apply_prewhere_after_final = 1);

-- A subcolumn is a bare reference too.
SELECT 'pw subcolumn count', count() FROM t_pw FINAL PREWHERE n.null SETTINGS apply_prewhere_after_final = 1;
SELECT 'pw subcolumn select k', sum(k) FROM (SELECT k FROM t_pw FINAL PREWHERE n.null SETTINGS apply_prewhere_after_final = 1);
SELECT 'pw subcolumn select star', count() FROM (SELECT * FROM t_pw FINAL PREWHERE n.null SETTINGS apply_prewhere_after_final = 1);

-- SELECT * must return exactly the table's columns: keeping the predicate alive must not leak one.
SELECT 'star shape', * FROM t_pw FINAL PREWHERE b ORDER BY k LIMIT 2 SETTINGS apply_prewhere_after_final = 1;
SELECT 'star shape where', * FROM t_pw FINAL WHERE b ORDER BY k LIMIT 2;

-- Wrapped predicates have a distinct result node and always worked; they must keep working.
SELECT 'ctl not b', count() FROM t_pw FINAL PREWHERE NOT b SETTINGS apply_prewhere_after_final = 1;
SELECT 'ctl b = 1', count() FROM t_pw FINAL PREWHERE b = 1 SETTINGS apply_prewhere_after_final = 1;
SELECT 'ctl materialize', count() FROM t_pw FINAL PREWHERE materialize(b) SETTINGS apply_prewhere_after_final = 1;
SELECT 'ctl b and other', count() FROM t_pw FINAL PREWHERE b AND other SETTINGS apply_prewhere_after_final = 1;

-- Controls that never defer. These live on a table with no row policy on purpose.
SELECT 'ctl no final', count() FROM t_pw PREWHERE b;
SELECT 'ctl not deferred', count() FROM t_pw FINAL PREWHERE b SETTINGS apply_prewhere_after_final = 0;

DROP TABLE t_pw;

-- A bare-column row policy defers the same way and needs no SETTINGS clause at all.
DROP TABLE IF EXISTS t_rp;
CREATE TABLE t_rp (k UInt64, b UInt8, other UInt8, n Nullable(UInt8)) ENGINE = ReplacingMergeTree() ORDER BY k;
INSERT INTO t_rp SELECT number, number % 2, number % 3, if(number % 4 = 0, NULL, number) FROM numbers(20);
DROP ROW POLICY IF EXISTS 04882_policy_b ON t_rp;
CREATE ROW POLICY 04882_policy_b ON t_rp USING b TO ALL;

SELECT 'policy count', count() FROM t_rp FINAL;
SELECT 'policy select k', sum(k) FROM (SELECT k FROM t_rp FINAL);
SELECT 'policy select star', count() FROM (SELECT * FROM t_rp FINAL);
SELECT 'policy star shape', * FROM t_rp FINAL ORDER BY k LIMIT 2;
-- Stacked: a deferred policy and a deferred PREWHERE, same column and different columns. Each is
-- paired with the equivalent WHERE over the undeferred policy, so a wrong count is caught too.
SELECT 'policy and pw same column', count() FROM t_rp FINAL PREWHERE b SETTINGS apply_prewhere_after_final = 1;
SELECT 'policy and pw same column gt', count() FROM t_rp FINAL WHERE b SETTINGS apply_row_policy_after_final = 0;
SELECT 'policy and pw other column', count() FROM t_rp FINAL PREWHERE other SETTINGS apply_prewhere_after_final = 1;
SELECT 'policy and pw other column gt', count() FROM t_rp FINAL WHERE b AND other SETTINGS apply_row_policy_after_final = 0;
SELECT 'policy not deferred', count() FROM t_rp FINAL SETTINGS apply_row_policy_after_final = 0;

DROP ROW POLICY 04882_policy_b ON t_rp;
DROP TABLE t_rp;

-- A bare subcolumn as the policy expression. Column ov overlaps the policy so the stacked arms
-- below survive rows rather than trivially returning zero.
DROP TABLE IF EXISTS t_rp_sub;
CREATE TABLE t_rp_sub (k UInt64, b UInt8, ov UInt8, n Nullable(UInt8)) ENGINE = ReplacingMergeTree() ORDER BY k;
INSERT INTO t_rp_sub SELECT number, number % 2, number % 8 = 0, if(number % 4 = 0, NULL, number) FROM numbers(20);
DROP ROW POLICY IF EXISTS 04882_policy_sub ON t_rp_sub;
CREATE ROW POLICY 04882_policy_sub ON t_rp_sub USING n.null TO ALL;

SELECT 'policy subcolumn count', count() FROM t_rp_sub FINAL;
SELECT 'policy subcolumn select k', sum(k) FROM (SELECT k FROM t_rp_sub FINAL);
-- Deferred bare subcolumn policy stacked with a deferred bare column PREWHERE. The expected count
-- is computed from the source data, independently of any read path.
SELECT 'policy subcolumn and pw', count() FROM t_rp_sub FINAL PREWHERE ov SETTINGS apply_prewhere_after_final = 1;
SELECT 'policy subcolumn and pw gt', countIf(n IS NULL AND ov)
FROM (SELECT if(number % 4 = 0, NULL, number) AS n, number % 8 = 0 AS ov FROM numbers(20));

DROP ROW POLICY 04882_policy_sub ON t_rp_sub;
DROP TABLE t_rp_sub;

-- The issue's own reproducer: a policy on one column defers a PREWHERE on another.
DROP TABLE IF EXISTS t_issue;
CREATE TABLE t_issue (k UInt64, b UInt8, other UInt8) ENGINE = ReplacingMergeTree() ORDER BY k;
INSERT INTO t_issue SELECT number, number % 2, number % 3 FROM numbers(20);
DROP ROW POLICY IF EXISTS 04882_policy_other ON t_issue;
CREATE ROW POLICY 04882_policy_other ON t_issue USING other = 1 TO ALL;

SELECT 'issue repro', count() FROM t_issue FINAL PREWHERE b;
SELECT 'issue ground truth', count() FROM t_issue FINAL WHERE b;

DROP ROW POLICY 04882_policy_other ON t_issue;
DROP TABLE t_issue;

-- Every FINAL-capable engine reaches the same plan code.
DROP TABLE IF EXISTS t_eng;
CREATE TABLE t_eng (k UInt64, b UInt8, other UInt8) ENGINE = CoalescingMergeTree() ORDER BY k;
INSERT INTO t_eng SELECT number, number % 2, number % 3 FROM numbers(20);
SELECT 'coalescing', count() FROM t_eng FINAL PREWHERE b SETTINGS apply_prewhere_after_final = 1;
DROP TABLE t_eng;

DROP TABLE IF EXISTS t_eng2;
CREATE TABLE t_eng2 (k UInt64, b UInt8, other UInt8) ENGINE = AggregatingMergeTree() ORDER BY k;
INSERT INTO t_eng2 SELECT number, number % 2, number % 3 FROM numbers(20);
SELECT 'aggregating', count() FROM t_eng2 FINAL PREWHERE b SETTINGS apply_prewhere_after_final = 1;
DROP TABLE t_eng2;

DROP TABLE IF EXISTS t_eng3;
CREATE TABLE t_eng3 (k UInt64, b UInt8, other UInt8) ENGINE = SummingMergeTree() ORDER BY k;
INSERT INTO t_eng3 SELECT number, number % 2, number % 3 FROM numbers(20);
SELECT 'summing', count() FROM t_eng3 FINAL PREWHERE b SETTINGS apply_prewhere_after_final = 1;
DROP TABLE t_eng3;

-- Type wrappers: the predicate is matched by name, so no wrapper is special.
DROP TABLE IF EXISTS t_wrap;
SET allow_suspicious_low_cardinality_types = 1;
CREATE TABLE t_wrap (k UInt64, nu Nullable(UInt8), lc LowCardinality(UInt8), lcn LowCardinality(Nullable(UInt8)), f Float64)
ENGINE = ReplacingMergeTree() ORDER BY k;
INSERT INTO t_wrap SELECT number, if(number % 4 = 0, NULL, number % 2), number % 2, if(number % 4 = 0, NULL, number % 2), number % 2 FROM numbers(20);

SELECT 'wrapper nullable', count() FROM t_wrap FINAL PREWHERE nu SETTINGS apply_prewhere_after_final = 1;
SELECT 'wrapper lowcardinality', count() FROM t_wrap FINAL PREWHERE lc SETTINGS apply_prewhere_after_final = 1;
SELECT 'wrapper lc nullable', count() FROM t_wrap FINAL PREWHERE lcn SETTINGS apply_prewhere_after_final = 1;
SELECT 'wrapper float', count() FROM t_wrap FINAL PREWHERE f SETTINGS apply_prewhere_after_final = 1;

DROP TABLE t_wrap;

-- The filter must run after FINAL, on the surviving row, and read the real source column.
-- Key 1 passes only before deduplication, key 2 only after, so the two orders must disagree.
-- Merges are stopped per table because a merge would collapse the versions and remove the flip.
DROP TABLE IF EXISTS t_ver;
CREATE TABLE t_ver (k UInt64, b UInt8, other UInt8, ver UInt64) ENGINE = ReplacingMergeTree(ver) ORDER BY k;
SYSTEM STOP MERGES t_ver;
INSERT INTO t_ver VALUES (1, 1, 1, 1), (2, 0, 1, 1), (3, 1, 1, 1);
INSERT INTO t_ver VALUES (1, 0, 1, 2), (2, 7, 1, 2), (3, 1, 1, 2);

SELECT 'flip pw deferred', count() FROM t_ver FINAL PREWHERE b SETTINGS apply_prewhere_after_final = 1;
SELECT 'flip pw before final', count() FROM t_ver FINAL PREWHERE b SETTINGS apply_prewhere_after_final = 0;
SELECT 'flip pw deferred keys', sum(k) FROM (SELECT k FROM t_ver FINAL PREWHERE b SETTINGS apply_prewhere_after_final = 1);
SELECT 'flip pw before final keys', sum(k) FROM (SELECT k FROM t_ver FINAL PREWHERE b SETTINGS apply_prewhere_after_final = 0);
-- b = 7 is the surviving source value, so a synthesized truth column would be visible here.
SELECT 'flip pw values', k, b FROM t_ver FINAL PREWHERE b ORDER BY k SETTINGS apply_prewhere_after_final = 1;

DROP TABLE t_ver;

-- Same flip for a bare-column row policy, on the shape that needs no SETTINGS clause.
DROP TABLE IF EXISTS t_ver_rp;
CREATE TABLE t_ver_rp (k UInt64, b UInt8, other UInt8, ver UInt64) ENGINE = ReplacingMergeTree(ver) ORDER BY k;
SYSTEM STOP MERGES t_ver_rp;
INSERT INTO t_ver_rp VALUES (1, 1, 1, 1), (2, 0, 1, 1), (3, 1, 1, 1);
INSERT INTO t_ver_rp VALUES (1, 0, 1, 2), (2, 7, 1, 2), (3, 1, 1, 2);
DROP ROW POLICY IF EXISTS 04882_policy_flip ON t_ver_rp;
CREATE ROW POLICY 04882_policy_flip ON t_ver_rp USING b TO ALL;

SELECT 'flip policy deferred', count() FROM t_ver_rp FINAL;
SELECT 'flip policy before final', count() FROM t_ver_rp FINAL SETTINGS apply_row_policy_after_final = 0;
SELECT 'flip policy values', k, b FROM t_ver_rp FINAL ORDER BY k;

DROP ROW POLICY 04882_policy_flip ON t_ver_rp;
DROP TABLE t_ver_rp;
