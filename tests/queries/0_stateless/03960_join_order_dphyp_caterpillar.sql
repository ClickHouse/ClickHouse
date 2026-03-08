-- Tags: long, no-debug, no-sanitizers, no-sanitize-coverage

-- Test: Caterpillar graph - DPhyp is O(K) while DPsize is O(2^K)
--
-- Topology: S1 - S2 - ... - SK   (spine chain)
--            |    |          |
--           L1   L2  ...   LK    (pendant leaves)
--
-- Each spine node Si has at most 3 neighbours (S(i-1), S(i+1), Li).
-- Each leaf Li has exactly 1 neighbour (Si).
-- DPhyp cost: O(K * (2^3 + 2^1)) = O(10K)   - nearly constant per node.
--
-- Connected subgraphs: every contiguous spine interval [Si..Sj] may include
-- or exclude each of its j-i+1 pendant leaves independently, giving
--   |connected_subgraphs| ~ sum (K-l+1)*2^l  ~  2^(K+2)
-- DPsize cost: O(|connected_subgraphs|^2)  = O(4^K)
--
--   K = 8  (16 nodes): |subgraphs| ~  1 004, DPsize ~  500 K pairs  -> fast.
--   K = 15 (30 nodes): |subgraphs| ~ 131 K, DPsize ~ 8.6 B pairs  -> impractical.
--
-- At K = 15 the test runs only DPhyp (and greedy for correctness).
-- Running DPsize at K = 15 is intentionally omitted to avoid CI timeouts.

SET allow_experimental_analyzer = 1;
SET use_statistics = 1;
SET query_plan_join_swap_table = 'auto';
SET enable_join_runtime_filters = 0;

-- Single table reused under many aliases to express the caterpillar topology
-- without 30 separate CREATE TABLE statements.
--
-- Spine edges:  s(i+1).id = s(i).id + 1
-- Pendant edges: l(i).id  = s(i).id + 100
--
-- Both kinds produce a proper binary hyperedge (asBinaryPredicate returns
-- (Equals, lhs, rhs) with lhs.sources and rhs.sources each a single alias).
-- The filter  s1.id BETWEEN 0 AND 9  is a push-down filter on s1 (~10 rows)
-- that is not a join edge.

CREATE TABLE cat_node (id UInt32, val UInt32)
ENGINE = MergeTree() PRIMARY KEY id
SETTINGS auto_statistics_types = 'uniq';

INSERT INTO cat_node SELECT number, number FROM numbers(200);

-- ==========================================================================
-- K = 8 caterpillar (16 nodes, limit = 16)
-- Both DPhyp and DPsize finish quickly here; they must return the same hash.
-- DPsize processes |subgraphs|^2/2 ~ 500K candidate pairs.
-- DPhyp processes K*10 = 80 inner iterations.
-- ==========================================================================
SELECT 'K=8 caterpillar: DPhyp result';
SELECT sum(sipHash64(s1.id, s8.id, l1.id, l8.id))
FROM cat_node s1, cat_node l1, cat_node s2, cat_node l2,
     cat_node s3, cat_node l3, cat_node s4, cat_node l4,
     cat_node s5, cat_node l5, cat_node s6, cat_node l6,
     cat_node s7, cat_node l7, cat_node s8, cat_node l8
WHERE s1.id BETWEEN 0 AND 9
  AND s2.id = s1.id + 1  AND s3.id = s2.id + 1  AND s4.id = s3.id + 1
  AND s5.id = s4.id + 1  AND s6.id = s5.id + 1  AND s7.id = s6.id + 1
  AND s8.id = s7.id + 1
  AND l1.id = s1.id + 100  AND l2.id = s2.id + 100  AND l3.id = s3.id + 100
  AND l4.id = s4.id + 100  AND l5.id = s5.id + 100  AND l6.id = s6.id + 100
  AND l7.id = s7.id + 100  AND l8.id = s8.id + 100
SETTINGS query_plan_optimize_join_order_algorithm = 'dphyp',
         query_plan_optimize_join_order_limit = 16,
         enable_parallel_replicas = 0;

SELECT 'K=8 caterpillar: DPsize result (must match)';
SELECT sum(sipHash64(s1.id, s8.id, l1.id, l8.id))
FROM cat_node s1, cat_node l1, cat_node s2, cat_node l2,
     cat_node s3, cat_node l3, cat_node s4, cat_node l4,
     cat_node s5, cat_node l5, cat_node s6, cat_node l6,
     cat_node s7, cat_node l7, cat_node s8, cat_node l8
WHERE s1.id BETWEEN 0 AND 9
  AND s2.id = s1.id + 1  AND s3.id = s2.id + 1  AND s4.id = s3.id + 1
  AND s5.id = s4.id + 1  AND s6.id = s5.id + 1  AND s7.id = s6.id + 1
  AND s8.id = s7.id + 1
  AND l1.id = s1.id + 100  AND l2.id = s2.id + 100  AND l3.id = s3.id + 100
  AND l4.id = s4.id + 100  AND l5.id = s5.id + 100  AND l6.id = s6.id + 100
  AND l7.id = s7.id + 100  AND l8.id = s8.id + 100
SETTINGS query_plan_optimize_join_order_algorithm = 'dpsize',
         query_plan_optimize_join_order_limit = 16,
         enable_parallel_replicas = 0;

-- ==========================================================================
-- K = 15 caterpillar (30 nodes, limit = 30)
-- DPsize is NOT run at this scale (~8.6B candidate pairs).
-- DPhyp processes K*10 = 150 inner iterations; result is verified against greedy.
-- ==========================================================================
SELECT 'K=15 caterpillar: DPhyp (limit=30)';
SELECT count()
FROM cat_node s1,  cat_node l1,  cat_node s2,  cat_node l2,
     cat_node s3,  cat_node l3,  cat_node s4,  cat_node l4,
     cat_node s5,  cat_node l5,  cat_node s6,  cat_node l6,
     cat_node s7,  cat_node l7,  cat_node s8,  cat_node l8,
     cat_node s9,  cat_node l9,  cat_node s10, cat_node l10,
     cat_node s11, cat_node l11, cat_node s12, cat_node l12,
     cat_node s13, cat_node l13, cat_node s14, cat_node l14,
     cat_node s15, cat_node l15
WHERE s1.id BETWEEN 0 AND 9
  AND s2.id  = s1.id  + 1  AND s3.id  = s2.id  + 1  AND s4.id  = s3.id  + 1
  AND s5.id  = s4.id  + 1  AND s6.id  = s5.id  + 1  AND s7.id  = s6.id  + 1
  AND s8.id  = s7.id  + 1  AND s9.id  = s8.id  + 1  AND s10.id = s9.id  + 1
  AND s11.id = s10.id + 1  AND s12.id = s11.id + 1  AND s13.id = s12.id + 1
  AND s14.id = s13.id + 1  AND s15.id = s14.id + 1
  AND l1.id  = s1.id  + 100  AND l2.id  = s2.id  + 100  AND l3.id  = s3.id  + 100
  AND l4.id  = s4.id  + 100  AND l5.id  = s5.id  + 100  AND l6.id  = s6.id  + 100
  AND l7.id  = s7.id  + 100  AND l8.id  = s8.id  + 100  AND l9.id  = s9.id  + 100
  AND l10.id = s10.id + 100  AND l11.id = s11.id + 100  AND l12.id = s12.id + 100
  AND l13.id = s13.id + 100  AND l14.id = s14.id + 100  AND l15.id = s15.id + 100
SETTINGS query_plan_optimize_join_order_algorithm = 'dphyp',
         query_plan_optimize_join_order_limit = 30,
         enable_parallel_replicas = 0;

SELECT 'K=15 caterpillar: greedy (must match)';
SELECT count()
FROM cat_node s1,  cat_node l1,  cat_node s2,  cat_node l2,
     cat_node s3,  cat_node l3,  cat_node s4,  cat_node l4,
     cat_node s5,  cat_node l5,  cat_node s6,  cat_node l6,
     cat_node s7,  cat_node l7,  cat_node s8,  cat_node l8,
     cat_node s9,  cat_node l9,  cat_node s10, cat_node l10,
     cat_node s11, cat_node l11, cat_node s12, cat_node l12,
     cat_node s13, cat_node l13, cat_node s14, cat_node l14,
     cat_node s15, cat_node l15
WHERE s1.id BETWEEN 0 AND 9
  AND s2.id  = s1.id  + 1  AND s3.id  = s2.id  + 1  AND s4.id  = s3.id  + 1
  AND s5.id  = s4.id  + 1  AND s6.id  = s5.id  + 1  AND s7.id  = s6.id  + 1
  AND s8.id  = s7.id  + 1  AND s9.id  = s8.id  + 1  AND s10.id = s9.id  + 1
  AND s11.id = s10.id + 1  AND s12.id = s11.id + 1  AND s13.id = s12.id + 1
  AND s14.id = s13.id + 1  AND s15.id = s14.id + 1
  AND l1.id  = s1.id  + 100  AND l2.id  = s2.id  + 100  AND l3.id  = s3.id  + 100
  AND l4.id  = s4.id  + 100  AND l5.id  = s5.id  + 100  AND l6.id  = s6.id  + 100
  AND l7.id  = s7.id  + 100  AND l8.id  = s8.id  + 100  AND l9.id  = s9.id  + 100
  AND l10.id = s10.id + 100  AND l11.id = s11.id + 100  AND l12.id = s12.id + 100
  AND l13.id = s13.id + 100  AND l14.id = s14.id + 100  AND l15.id = s15.id + 100
SETTINGS query_plan_optimize_join_order_algorithm = 'greedy',
         query_plan_optimize_join_order_limit = 30,
         enable_parallel_replicas = 0;

-- ==========================================================================
-- K = 15 caterpillar (30 nodes): DPsize must time out
-- DPsize enumerates ~8.6B candidate pairs; it must not complete within 15 s.
-- This confirms the exponential blowup that DPhyp avoids.
-- ==========================================================================
SELECT 'K=15 caterpillar: DPsize (must time out)';
SELECT count()
FROM cat_node s1,  cat_node l1,  cat_node s2,  cat_node l2,
     cat_node s3,  cat_node l3,  cat_node s4,  cat_node l4,
     cat_node s5,  cat_node l5,  cat_node s6,  cat_node l6,
     cat_node s7,  cat_node l7,  cat_node s8,  cat_node l8,
     cat_node s9,  cat_node l9,  cat_node s10, cat_node l10,
     cat_node s11, cat_node l11, cat_node s12, cat_node l12,
     cat_node s13, cat_node l13, cat_node s14, cat_node l14,
     cat_node s15, cat_node l15
WHERE s1.id BETWEEN 0 AND 9
  AND s2.id  = s1.id  + 1  AND s3.id  = s2.id  + 1  AND s4.id  = s3.id  + 1
  AND s5.id  = s4.id  + 1  AND s6.id  = s5.id  + 1  AND s7.id  = s6.id  + 1
  AND s8.id  = s7.id  + 1  AND s9.id  = s8.id  + 1  AND s10.id = s9.id  + 1
  AND s11.id = s10.id + 1  AND s12.id = s11.id + 1  AND s13.id = s12.id + 1
  AND s14.id = s13.id + 1  AND s15.id = s14.id + 1
  AND l1.id  = s1.id  + 100  AND l2.id  = s2.id  + 100  AND l3.id  = s3.id  + 100
  AND l4.id  = s4.id  + 100  AND l5.id  = s5.id  + 100  AND l6.id  = s6.id  + 100
  AND l7.id  = s7.id  + 100  AND l8.id  = s8.id  + 100  AND l9.id  = s9.id  + 100
  AND l10.id = s10.id + 100  AND l11.id = s11.id + 100  AND l12.id = s12.id + 100
  AND l13.id = s13.id + 100  AND l14.id = s14.id + 100  AND l15.id = s15.id + 100
SETTINGS query_plan_optimize_join_order_algorithm = 'dpsize',
         query_plan_optimize_join_order_limit = 30,
         enable_parallel_replicas = 0,
         max_execution_time = 15; -- { serverError TIMEOUT_EXCEEDED }

DROP TABLE cat_node;
