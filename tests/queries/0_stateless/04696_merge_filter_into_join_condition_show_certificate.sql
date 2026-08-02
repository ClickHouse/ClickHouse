-- Tags: no-fasttest
-- no-fasttest: showCertificate throws SUPPORT_IS_DISABLED unless the build has OpenSSL, and the
-- fast-test build sets ENABLE_LIBRARIES=0 and does not check out contrib/openssl.

-- { echo }

-- This is the showCertificate row of 04689, split out because only that build capability differs.
-- showCertificate reads the certificate of the node its function object was built on -- the one the
-- client presented, or else the LOCAL server's own via `SSLManager::defaultServerContext` -- so two
-- nodes in a distributed query legitimately return different values. It overrides none of the four
-- predicates the guard reads, so only the name list can refuse it. Unlike the other zero-argument
-- members of this class it needs NO argument to stay live: it returns a `Map(String, String)`, and
-- constant folding requires a `ColumnConst`, so the node survives into the pass as a FUNCTION.

SET enable_analyzer = 1;                          -- the pass only sees JoinStepLogical
SET enable_parallel_replicas = 0;                 -- ditto
SET query_plan_join_swap_table = 0;               -- a swap changes which side is which
SET query_plan_optimize_join_order_randomize = 0; -- the plan-shape row asserts on join order
SET enable_join_runtime_filters = 0;              -- a runtime filter adds terms to the plan text
SET explain_query_plan_default = 'legacy';        -- `Clauses:` is only printed by the legacy format

CREATE TABLE l (k UInt32, a UInt32) ENGINE = Log;
CREATE TABLE r (k UInt32, b UInt8) ENGINE = Log;
INSERT INTO l SELECT number % 16, number FROM numbers(20000);
INSERT INTO r SELECT number % 16, number % 16 FROM numbers(320);

-- Positive control, same JOIN shape with a deterministic equality: the pass must still promote it.
-- Without this row a no-op pass, or one that refuses everything, would satisfy the refusal row below.
SELECT countIf(explain ILIKE '%Clauses:%' AND explain ILIKE '%__table1.k, %') = 1 FROM (
    EXPLAIN PLAN actions = 1 SELECT r.b FROM l INNER JOIN r ON l.k = r.k
    WHERE toUInt8(l.a % 16) = r.b
    SETTINGS query_plan_merge_filter_into_join_condition = 1);

-- The premise the row rests on: the call reaches the pass as a live FUNCTION rather than a folded
-- constant. Without it the row would pass whatever the guard does.
SELECT countIf(explain ILIKE '%showCertificate%') > 0 FROM (
    EXPLAIN PLAN actions = 1 SELECT r.b FROM l INNER JOIN r ON l.k = r.k
    WHERE toUInt8((length(mapKeys(showCertificate())) + l.a) % 16) = r.b
    SETTINGS query_plan_merge_filter_into_join_condition = 1);

SELECT countIf(explain ILIKE '%Clauses:%' AND explain ILIKE '%__table1.k, %') = 0 FROM (
    EXPLAIN PLAN actions = 1 SELECT r.b FROM l INNER JOIN r ON l.k = r.k
    WHERE toUInt8((length(mapKeys(showCertificate())) + l.a) % 16) = r.b
    SETTINGS query_plan_merge_filter_into_join_condition = 1);

DROP TABLE l;
DROP TABLE r;
