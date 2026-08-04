-- Tags: no-fasttest
-- ^ cas is an object-storage metadata type; keep it off the minimal fasttest image.

-- Projections on a cas disk: the projection's files are stored as nested keys
-- (<proj>.proj/<file>) in the parent part's manifest. Verify INSERT writes a projection, a
-- projection-optimized SELECT returns correct results, and a merge (OPTIMIZE FINAL) rebuilds it.

DROP TABLE IF EXISTS t_proj_cas;

CREATE TABLE t_proj_cas (a UInt64, b UInt64, PROJECTION p_by_b (SELECT a, b ORDER BY b))
ENGINE = MergeTree ORDER BY a
SETTINGS disk = disk(
    type = object_storage,
    object_storage_type = local,
    metadata_type = cas,
    server_root_id = '04299',
    name = '04299_cas_projection',
    path = '04299_cas_projection_pool/');

INSERT INTO t_proj_cas SELECT number, number % 10 FROM numbers(1000);
INSERT INTO t_proj_cas SELECT number, number % 10 FROM numbers(1000, 1000);

SELECT 'count', count() FROM t_proj_cas;
SELECT 'sum_b', sum(b) FROM t_proj_cas;
SELECT 'by_b', b, count() FROM t_proj_cas GROUP BY b ORDER BY b;

OPTIMIZE TABLE t_proj_cas FINAL;
SELECT 'after_merge_count', count() FROM t_proj_cas;
SELECT 'after_merge_by_b', b, count() FROM t_proj_cas GROUP BY b ORDER BY b;

SELECT 'has_projection', countDistinct(name) FROM system.projection_parts
WHERE database = currentDatabase() AND table = 't_proj_cas' AND active;

-- Prove the projection is actually selected by the optimizer (not a silent base-table fallback).
SET optimize_use_projections = 1, force_optimize_projection = 1;
SELECT 'uses_projection', countIf(explain LIKE '%p_by_b%') > 0
FROM (EXPLAIN actions = 1 SELECT b, count() FROM t_proj_cas GROUP BY b);

DROP TABLE t_proj_cas;

-- ALTER ADD/DROP/MATERIALIZE PROJECTION + DETACH/ATTACH durability on the cas disk. We use
-- the server's default cas storage policy here rather than an inline `disk = disk(...)`
-- definition: an ALTER runs `checkColumnFilenamesForCollision`, which re-applies the table's raw
-- `settings_changes` AST through the generic settings path, and the inline `disk(...)` function value
-- is a CustomType that cannot be assigned to the String `disk` setting there (BAD_GET). That is a
-- pre-existing, metadata-type-independent inline-disk-vs-ALTER issue, unrelated to content addressing;
-- the projection ALTER mechanics on the CA disk are identical with the default-disk table. On the
-- cas-default test job this plain table lands on a CA disk; on the normal job it lands on
-- the local disk. The expected values below are the same on both (the oracle) — that equivalence is the
-- whole point of B58: a merge/mutate-rebuilt projection must survive a reload on CA exactly as on a
-- normal disk.
DROP TABLE IF EXISTS t_proj_cas_alter;

CREATE TABLE t_proj_cas_alter (a UInt64, b UInt64, PROJECTION p_by_b (SELECT a, b ORDER BY b))
ENGINE = MergeTree ORDER BY a;

INSERT INTO t_proj_cas_alter SELECT number, number % 10 FROM numbers(1000);
INSERT INTO t_proj_cas_alter SELECT number, number % 10 FROM numbers(1000, 1000);
OPTIMIZE TABLE t_proj_cas_alter FINAL;

-- B58 DURABILITY (merge): the merge-rebuilt projection must survive a DETACH/ATTACH — it must live in the
-- committed manifest, not only in memory. Reload and assert the projection is still active and usable.
DETACH TABLE t_proj_cas_alter;
ATTACH TABLE t_proj_cas_alter;
SELECT 'after_merge_reload_projection', countDistinct(name) FROM system.projection_parts
WHERE database = currentDatabase() AND table = 't_proj_cas_alter' AND active;
SET optimize_use_projections = 1, force_optimize_projection = 1;
SELECT 'after_merge_reload_uses_projection', countIf(explain LIKE '%p_by_b%') > 0
FROM (EXPLAIN actions = 1 SELECT b, count() FROM t_proj_cas_alter GROUP BY b);
SET force_optimize_projection = 0;

-- ALTER ADD PROJECTION on an existing table, then MATERIALIZE it on existing parts (rebuild path).
-- This exercises the temp-projection (<proj>.tmp_proj -> <proj>.proj) flow inside the mutated part on
-- the CA disk.
ALTER TABLE t_proj_cas_alter ADD PROJECTION p_sum (SELECT b, sum(a) GROUP BY b);
ALTER TABLE t_proj_cas_alter MATERIALIZE PROJECTION p_sum SETTINGS mutations_sync = 2;
SELECT 'after_add_projection_count', count() FROM t_proj_cas_alter;
-- After MATERIALIZE both the pre-existing p_by_b and the freshly built p_sum must be active. B58: the
-- mutation must carry p_by_b forward and persist p_sum into the manifest of the rebuilt part.
SELECT 'projections_after_add', name, count() FROM system.projection_parts
WHERE database = currentDatabase() AND table = 't_proj_cas_alter' AND active GROUP BY name ORDER BY name;

-- The newly materialized projection must actually be selected by the optimizer.
SELECT 'uses_p_sum', countIf(explain LIKE '%p_sum%') > 0
FROM (EXPLAIN actions = 1 SELECT b, sum(a) FROM t_proj_cas_alter GROUP BY b);

-- B58 DURABILITY (materialize): both projections must survive a DETACH/ATTACH after MATERIALIZE.
DETACH TABLE t_proj_cas_alter;
ATTACH TABLE t_proj_cas_alter;
SELECT 'projections_after_materialize_reload', name, count() FROM system.projection_parts
WHERE database = currentDatabase() AND table = 't_proj_cas_alter' AND active GROUP BY name ORDER BY name;
SET optimize_use_projections = 1, force_optimize_projection = 1;
SELECT 'after_materialize_reload_uses_p_sum', countIf(explain LIKE '%p_sum%') > 0
FROM (EXPLAIN actions = 1 SELECT b, sum(a) FROM t_proj_cas_alter GROUP BY b);
SET force_optimize_projection = 0;

-- B58 DURABILITY (data mutation): a mutation rebuilds the part; the surviving projections must be carried
-- into the mutated part's manifest and stay usable after a reload. We use a DELETE that matches no rows so
-- the row data (and therefore every expected value below) is unchanged across CA and non-CA — the part is
-- still fully rewritten, exercising the mutation projection path.
ALTER TABLE t_proj_cas_alter DELETE WHERE b = 999 SETTINGS mutations_sync = 2;
DETACH TABLE t_proj_cas_alter;
ATTACH TABLE t_proj_cas_alter;
SELECT 'projections_after_update_reload', name, count() FROM system.projection_parts
WHERE database = currentDatabase() AND table = 't_proj_cas_alter' AND active GROUP BY name ORDER BY name;
SET optimize_use_projections = 1, force_optimize_projection = 1;
SELECT 'after_update_reload_uses_p_sum', countIf(explain LIKE '%p_sum%') > 0
FROM (EXPLAIN actions = 1 SELECT b, sum(a) FROM t_proj_cas_alter GROUP BY b);
SET force_optimize_projection = 0;

-- DROP a projection: results unchanged, the projection's nested keys leave the new part version.
ALTER TABLE t_proj_cas_alter DROP PROJECTION p_by_b SETTINGS mutations_sync = 2;
SELECT 'after_drop_projection_count', count() FROM t_proj_cas_alter;
SELECT 'projections_after_drop', name, count() FROM system.projection_parts
WHERE database = currentDatabase() AND table = 't_proj_cas_alter' AND active GROUP BY name ORDER BY name;

-- Persistence: reload from the disk and re-read. `p_by_b` is gone, so the count() query falls back to the
-- base table; the surviving `p_sum` still serves the sum(a) aggregation after the reload.
DETACH TABLE t_proj_cas_alter;
ATTACH TABLE t_proj_cas_alter;
SELECT 'after_reload_by_b', b, count() FROM t_proj_cas_alter GROUP BY b ORDER BY b;
SELECT 'after_reload_sum_b', b, sum(a) FROM t_proj_cas_alter GROUP BY b ORDER BY b;

DROP TABLE t_proj_cas_alter;
SELECT 'dropped_ok';
