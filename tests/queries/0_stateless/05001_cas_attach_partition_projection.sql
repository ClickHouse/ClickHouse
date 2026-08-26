-- Tags: no-random-settings, no-random-merge-tree-settings

-- B64: DETACH PARTITION + ATTACH PARTITION of a part that has a projection. On a content-addressed
-- disk the part is re-attached from its detached STAGING directory (detached/attaching_<part>/), so
-- the projection sub-directory is read as the NESTED path detached/attaching_<part>/<proj>.proj. The
-- CA metadata storage recognized a projection directory only as a DIRECT child of a part
-- (<part>/<proj>.proj), so the nested staging shape was missed: existsDirectory("<proj>.proj") returned
-- false during the attach-time load, and IMergeTreeDataPart::loadProjections registered the surviving
-- projection part with EMPTY columns and rows_count == 0 — making it unusable (PROJECTION_NOT_USED) and
-- causing CHECK TABLE to throw BROKEN_PROJECTION (in-memory columns empty vs on-disk columns), even
-- though the on-disk projection data was intact. Same projection-on-CA family as B58/B63, on the
-- ATTACH-clone path. This oracle exercises DETACH+ATTACH PARTITION (no projection drop) and asserts the
-- surviving projection re-attaches with the correct rows, is usable, and CHECK TABLE passes. It is
-- correct on both a plain and a content-addressed default disk.

DROP TABLE IF EXISTS t_attach_proj;

CREATE TABLE t_attach_proj (x Int32, y Int32, PROJECTION p (SELECT x, y ORDER BY x))
ENGINE = MergeTree() PARTITION BY intDiv(y, 100) ORDER BY y;

INSERT INTO t_attach_proj SELECT number, number FROM numbers(7);

SELECT 'before_attach_rows', min(rows)
FROM system.projection_parts
WHERE database = currentDatabase() AND table = 't_attach_proj' AND active;

ALTER TABLE t_attach_proj DETACH PARTITION 0;
ALTER TABLE t_attach_proj ATTACH PARTITION 0;

-- The surviving projection must re-attach with the correct row count (rows > 0), not empty.
SELECT 'after_attach_rows', min(rows)
FROM system.projection_parts
WHERE database = currentDatabase() AND table = 't_attach_proj' AND active;

-- Base data must be intact.
SELECT 'data', count(), sum(x), sum(y) FROM t_attach_proj;

-- The projection must be usable: force_optimize_projection requires a projection to serve the query,
-- so this throws if the projection is broken/empty.
SELECT 'projection_served', x, y FROM t_attach_proj ORDER BY x
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

-- CHECK TABLE must pass (the projection's in-memory columns must match the on-disk columns).
CHECK TABLE t_attach_proj SETTINGS check_query_single_value_result = 1;

DROP TABLE t_attach_proj;
