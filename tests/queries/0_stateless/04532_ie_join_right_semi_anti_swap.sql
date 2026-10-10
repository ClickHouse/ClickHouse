-- Tags: no-old-analyzer

-- RIGHT SEMI/ANTI IEJoin is executed as its LEFT mirror: `IEJoinStep` swaps its input
-- pipelines, reverses the operators (EXPLAIN shows the executed type with `Swapped: true`),
-- and restores the original column order on top of the join. The scan then drives from the
-- side whose matches are contiguous and stops at the first match instead of deduplicating.
-- The band condition is crafted to have the same match set as the equality `swl.k + 10 = swr.k`,
-- so every result is cross-checked against a hash join of the same kind.

SET join_algorithm = 'direct,parallel_hash,hash,ie_join';
-- The join order optimizer may flip RIGHT to LEFT with swapped children on its own when
-- statistics suggest it; disable it so that the kind reaches the IEJoin planner code as RIGHT
-- and the checks below pin the IEJoin-level normalization.
SET query_plan_optimize_join_order_limit = 0;

DROP TABLE IF EXISTS swl;
DROP TABLE IF EXISTS swr;

CREATE TABLE swl (id Int32, k Nullable(Int32)) ENGINE = MergeTree ORDER BY id;
CREATE TABLE swr (id Int32, k Nullable(Int32), v String) ENGINE = MergeTree ORDER BY id;

INSERT INTO swl VALUES (1, 10), (2, 20), (3, 20), (4, 40);
-- rows 2 and 3 have equal keys: SEMI emits one row per right ROW, so both must appear
INSERT INTO swr VALUES (1, 10, 'r1'), (2, 20, 'r2'), (3, 20, 'r3'), (4, 30, 'r4'), (5, NULL, 'rnull');

SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT swr.id FROM swl RIGHT SEMI JOIN swr ON swl.k < swr.k AND swl.k > swr.k - 15) WHERE explain LIKE '%Type: LEFT SEMI%';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT swr.id FROM swl RIGHT SEMI JOIN swr ON swl.k < swr.k AND swl.k > swr.k - 15) WHERE explain LIKE '%Swapped: true%';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT swr.id FROM swl RIGHT ANTI JOIN swr ON swl.k < swr.k AND swl.k > swr.k - 15) WHERE explain LIKE '%Type: LEFT ANTI%';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT swr.id FROM swl RIGHT ANTI JOIN swr ON swl.k < swr.k AND swl.k > swr.k - 15) WHERE explain LIKE '%Swapped: true%';

SELECT 'right semi';
SELECT swl.k, swr.id, swr.k, swr.v FROM swl RIGHT SEMI JOIN swr ON swl.k < swr.k AND swl.k > swr.k - 15 ORDER BY ALL;
SELECT 'right anti';
SELECT swl.id, swl.k, swr.id, swr.k, swr.v FROM swl RIGHT ANTI JOIN swr ON swl.k < swr.k AND swl.k > swr.k - 15 ORDER BY ALL;
SELECT 'right anti, join_use_nulls = 1';
SELECT swl.id, swl.k, swr.id, swr.k, swr.v FROM swl RIGHT ANTI JOIN swr ON swl.k < swr.k AND swl.k > swr.k - 15 ORDER BY ALL SETTINGS join_use_nulls = 1;

SELECT (SELECT arraySort(groupArray((swl.k, swr.id, swr.k, swr.v))) FROM swl RIGHT SEMI JOIN swr ON swl.k < swr.k AND swl.k > swr.k - 15)
     = (SELECT arraySort(groupArray((swl.k, swr.id, swr.k, swr.v))) FROM swl RIGHT SEMI JOIN swr ON swl.k + 10 = swr.k);
SELECT (SELECT arraySort(groupArray((swl.id, swl.k, swr.id, swr.k, swr.v))) FROM swl RIGHT ANTI JOIN swr ON swl.k < swr.k AND swl.k > swr.k - 15)
     = (SELECT arraySort(groupArray((swl.id, swl.k, swr.id, swr.k, swr.v))) FROM swl RIGHT ANTI JOIN swr ON swl.k + 10 = swr.k);

DROP TABLE swl;
DROP TABLE swr;
