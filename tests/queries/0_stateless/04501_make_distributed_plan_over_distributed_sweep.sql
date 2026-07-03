-- Correctness sweep for make_distributed_plan = 1 over a Distributed table.
-- Every case is executed twice: through the plan-level distributed read and with
-- make_distributed_plan = 0 (the legacy path). The reference asserts that both
-- blocks are identical.

SET enable_analyzer = 1;
SET make_distributed_plan = 1;

DROP TABLE IF EXISTS sweep_local;
DROP TABLE IF EXISTS sweep_dist;
DROP TABLE IF EXISTS sweep_dim;
DROP TABLE IF EXISTS sweep_repl_local;
DROP TABLE IF EXISTS sweep_repl_dist;
DROP TABLE IF EXISTS sweep_smpl_local;
DROP TABLE IF EXISTS sweep_smpl_dist;
DROP TABLE IF EXISTS sweep_merge;

CREATE TABLE sweep_local (k UInt64, v UInt64, s String) ENGINE = MergeTree ORDER BY k;
CREATE TABLE sweep_dist AS sweep_local ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), sweep_local);
INSERT INTO sweep_local SELECT number % 5, number, toString(number % 3) FROM numbers(30);

CREATE TABLE sweep_dim (k UInt64, name String) ENGINE = MergeTree ORDER BY k;
INSERT INTO sweep_dim SELECT number, concat('name_', toString(number)) FROM numbers(3);

SELECT '-- expressions and aliases in SELECT + WHERE';
SELECT k, v * 2 AS d, concat(s, '_x') AS t FROM sweep_dist WHERE d > 40 AND k % 2 = 0 ORDER BY k, d, t;
SELECT k, v * 2 AS d, concat(s, '_x') AS t FROM sweep_dist WHERE d > 40 AND k % 2 = 0 ORDER BY k, d, t SETTINGS make_distributed_plan = 0;

SELECT '-- GROUP BY + HAVING (aggregation on the initiator)';
SELECT k, count() AS c, sum(v) AS sv FROM sweep_dist GROUP BY k HAVING sv > 130 ORDER BY k;
SELECT k, count() AS c, sum(v) AS sv FROM sweep_dist GROUP BY k HAVING sv > 130 ORDER BY k SETTINGS make_distributed_plan = 0;

SELECT '-- ORDER BY + LIMIT/OFFSET';
SELECT k, v FROM sweep_dist ORDER BY v DESC, k LIMIT 5 OFFSET 2;
SELECT k, v FROM sweep_dist ORDER BY v DESC, k LIMIT 5 OFFSET 2 SETTINGS make_distributed_plan = 0;

SELECT '-- DISTINCT';
SELECT DISTINCT k, s FROM sweep_dist ORDER BY k, s;
SELECT DISTINCT k, s FROM sweep_dist ORDER BY k, s SETTINGS make_distributed_plan = 0;

SELECT '-- literal IN (set is pushed to the shards with the plan)';
SELECT count(), sum(v) FROM sweep_dist WHERE k IN (1, 3);
SELECT count(), sum(v) FROM sweep_dist WHERE k IN (1, 3) SETTINGS make_distributed_plan = 0;

SELECT '-- IN with a subquery over a local table (set is built on the initiator)';
SELECT count(), sum(v) FROM sweep_dist WHERE k IN (SELECT k FROM sweep_dim);
SELECT count(), sum(v) FROM sweep_dist WHERE k IN (SELECT k FROM sweep_dim) SETTINGS make_distributed_plan = 0;

-- distributed_product_mode = 'local' lets the legacy path accept the double-distributed subquery
-- (without it the legacy path rejects it, while the plan path builds the set on the initiator).
-- The two paths return equal results only because both shards of this localhost cluster read the
-- same underlying table, so the per-shard local-subquery rewrite sees identical data.
SELECT '-- IN with a subquery over the distributed table itself';
SELECT count(), sum(v) FROM sweep_dist WHERE k IN (SELECT k FROM sweep_dist WHERE v < 10) SETTINGS distributed_product_mode = 'local';
SELECT count(), sum(v) FROM sweep_dist WHERE k IN (SELECT k FROM sweep_dist WHERE v < 10) SETTINGS distributed_product_mode = 'local', make_distributed_plan = 0;

-- With SET distributed_product_mode = 'local' the plan path falls back to the legacy execution
-- path (the plan path cannot implement per-shard subquery rewriting), so make_distributed_plan = 1
-- and = 0 must agree. As above, the values match the classic per-shard local rewrite only because
-- both localhost shards read the same table.
SELECT '-- IN subquery over the Distributed table with SET distributed_product_mode = local (plan path falls back to legacy)';
SET distributed_product_mode = 'local';
SELECT count(), sum(v) FROM sweep_dist WHERE k IN (SELECT k FROM sweep_dist WHERE v < 10);
SELECT count(), sum(v) FROM sweep_dist WHERE k IN (SELECT k FROM sweep_dist WHERE v < 10) SETTINGS make_distributed_plan = 0;
SET distributed_product_mode = 'deny';

-- distributed_product_mode = 'allow' keeps the subquery as written on the legacy path (no rewrite),
-- so the plan path (initiator-side set from a `FutureSetFromSubquery` source plan holding another
-- placeholder) and the legacy path agree on this two-identical-shard cluster. This also documents
-- the divergence area: under the default 'deny' the legacy path throws while the plan path succeeds.
SELECT '-- IN subquery over the Distributed table with SET distributed_product_mode = allow';
SET distributed_product_mode = 'allow';
SELECT count(), sum(v) FROM sweep_dist WHERE k IN (SELECT k FROM sweep_dist WHERE v < 10);
SELECT count(), sum(v) FROM sweep_dist WHERE k IN (SELECT k FROM sweep_dist WHERE v < 10) SETTINGS make_distributed_plan = 0;
SET distributed_product_mode = 'deny';

SELECT '-- GLOBAL IN with a subquery over the distributed table';
SELECT count(), sum(v) FROM sweep_dist WHERE k GLOBAL IN (SELECT k FROM sweep_dist WHERE v < 10);
SELECT count(), sum(v) FROM sweep_dist WHERE k GLOBAL IN (SELECT k FROM sweep_dist WHERE v < 10) SETTINGS make_distributed_plan = 0;

SELECT '-- CTE whose body reads the Distributed table';
WITH agg AS (SELECT k, sum(v) AS sv FROM sweep_dist GROUP BY k) SELECT k, sv FROM agg WHERE sv > 150 ORDER BY k;
WITH agg AS (SELECT k, sum(v) AS sv FROM sweep_dist GROUP BY k) SELECT k, sv FROM agg WHERE sv > 150 ORDER BY k SETTINGS make_distributed_plan = 0;

SELECT '-- FINAL over a ReplacingMergeTree-backed Distributed table';
CREATE TABLE sweep_repl_local (k UInt64, v UInt64) ENGINE = ReplacingMergeTree ORDER BY k;
CREATE TABLE sweep_repl_dist AS sweep_repl_local ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), sweep_repl_local);
INSERT INTO sweep_repl_local SELECT number, number FROM numbers(5);
INSERT INTO sweep_repl_local SELECT number, number + 100 FROM numbers(5);
SELECT k, v FROM sweep_repl_dist FINAL ORDER BY k, v;
SELECT k, v FROM sweep_repl_dist FINAL ORDER BY k, v SETTINGS make_distributed_plan = 0;

SELECT '-- SAMPLE over a sampled MergeTree-backed Distributed table';
CREATE TABLE sweep_smpl_local (k UInt64) ENGINE = MergeTree ORDER BY intHash32(k) SAMPLE BY intHash32(k);
CREATE TABLE sweep_smpl_dist AS sweep_smpl_local ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), sweep_smpl_local);
INSERT INTO sweep_smpl_local SELECT number FROM numbers(100);
SELECT count() FROM sweep_smpl_dist SAMPLE 1/2;
SELECT count() FROM sweep_smpl_dist SAMPLE 1/2 SETTINGS make_distributed_plan = 0;

SELECT '-- JOIN of the Distributed table with a small local table (join on the initiator)';
SELECT d.k, dim.name, sum(d.v) AS sv FROM sweep_dist AS d INNER JOIN sweep_dim AS dim ON d.k = dim.k GROUP BY d.k, dim.name ORDER BY d.k;
SELECT d.k, dim.name, sum(d.v) AS sv FROM sweep_dist AS d INNER JOIN sweep_dim AS dim ON d.k = dim.k GROUP BY d.k, dim.name ORDER BY d.k SETTINGS make_distributed_plan = 0;

SELECT '-- StorageMerge over the Distributed table';
CREATE TABLE sweep_merge (k UInt64, v UInt64, s String) ENGINE = Merge(currentDatabase(), '^sweep_dist$');
SELECT count(), sum(v) FROM sweep_merge;
SELECT count(), sum(v) FROM sweep_merge SETTINGS make_distributed_plan = 0;

DROP TABLE sweep_merge;
DROP TABLE sweep_smpl_dist;
DROP TABLE sweep_smpl_local;
DROP TABLE sweep_repl_dist;
DROP TABLE sweep_repl_local;
DROP TABLE sweep_dim;
DROP TABLE sweep_dist;
DROP TABLE sweep_local;
