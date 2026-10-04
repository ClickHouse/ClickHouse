DROP TABLE IF EXISTS t_ofsf;
CREATE TABLE t_ofsf (c0 UInt32, u Float64 DEFAULT 0) ENGINE = MergeTree ORDER BY c0 SAMPLE BY c0;
INSERT INTO t_ofsf SELECT number, 0 FROM numbers(100);

SYSTEM STOP MERGES t_ofsf;
SET mutations_sync = 0;
ALTER TABLE t_ofsf UPDATE u = _sample_factor WHERE 1;

-- Every preview expectation below equals its materialized counterpart, so the arms only exercise the
-- on-the-fly path while the mutation is unfinished.
SELECT 'pending', count() FROM system.mutations
WHERE database = currentDatabase() AND table = 't_ofsf' AND NOT is_done;

-- The pair is the oracle: the previewed `u` must be what the background mutation persists (1),
-- while the query's own `_sample_factor` must still be the query's factor (2).
SELECT 'preview sampled', any(u), any(_sample_factor) FROM t_ofsf SAMPLE 0.5 SETTINGS apply_mutations_on_fly = 1;
SELECT 'preview plain', any(u), any(_sample_factor) FROM t_ofsf SETTINGS apply_mutations_on_fly = 1;
-- The outer query does not name the virtual at all.
SELECT 'preview sampled no outer sf', any(u) FROM t_ofsf SAMPLE 0.5 SETTINGS apply_mutations_on_fly = 1;
-- Preview off: the mutation is not applied, so the column keeps its stored value.
SELECT 'preview off', any(u) FROM t_ofsf SAMPLE 0.5 SETTINGS apply_mutations_on_fly = 0;

SYSTEM START MERGES t_ofsf;
SET mutations_sync = 2;
ALTER TABLE t_ofsf DELETE WHERE 0;

SELECT 'materialized sampled', any(u), any(_sample_factor) FROM t_ofsf SAMPLE 0.5;
SELECT 'materialized plain', any(u), any(_sample_factor) FROM t_ofsf;

DROP TABLE t_ofsf;

-- Row-count carrier: with the query's factor the predicate is true for every row and the preview
-- drops the whole table.
DROP TABLE IF EXISTS t_ofsf_del;
CREATE TABLE t_ofsf_del (c0 UInt32) ENGINE = MergeTree ORDER BY c0 SAMPLE BY c0;
INSERT INTO t_ofsf_del SELECT number FROM numbers(100);

SYSTEM STOP MERGES t_ofsf_del;
SET mutations_sync = 0;
ALTER TABLE t_ofsf_del DELETE WHERE _sample_factor > 1;

SELECT 'pending', count() FROM system.mutations
WHERE database = currentDatabase() AND table = 't_ofsf_del' AND NOT is_done;

SELECT 'delete preview sampled', count() FROM t_ofsf_del SAMPLE 0.5 SETTINGS apply_mutations_on_fly = 1;
SELECT 'delete preview plain', count() FROM t_ofsf_del SETTINGS apply_mutations_on_fly = 1;

SYSTEM START MERGES t_ofsf_del;
SET mutations_sync = 2;
ALTER TABLE t_ofsf_del DELETE WHERE 0;

SELECT 'delete materialized sampled', count() FROM t_ofsf_del SAMPLE 0.5;
SELECT 'delete materialized plain', count() FROM t_ofsf_del;

DROP TABLE t_ofsf_del;

-- Inverse row-count carrier: 90 rows survive only when the filter runs with the factor the background
-- mutation sees. Either the query's factor reaching the predicate or the filter not running at all
-- leaves all 100.
DROP TABLE IF EXISTS t_ofsf_del_inv;
CREATE TABLE t_ofsf_del_inv (c0 UInt32) ENGINE = MergeTree ORDER BY c0 SAMPLE BY c0;
INSERT INTO t_ofsf_del_inv SELECT number FROM numbers(100);

SYSTEM STOP MERGES t_ofsf_del_inv;
SET mutations_sync = 0;
ALTER TABLE t_ofsf_del_inv DELETE WHERE _sample_factor = 1 AND c0 < 10;

SELECT 'pending', count() FROM system.mutations
WHERE database = currentDatabase() AND table = 't_ofsf_del_inv' AND NOT is_done;

SELECT 'delete inverse preview sampled', count() FROM t_ofsf_del_inv SAMPLE 0.5 SETTINGS apply_mutations_on_fly = 1;
SELECT 'delete inverse preview plain', count() FROM t_ofsf_del_inv SETTINGS apply_mutations_on_fly = 1;

SYSTEM START MERGES t_ofsf_del_inv;
SET mutations_sync = 2;
ALTER TABLE t_ofsf_del_inv DELETE WHERE 0;

SELECT 'delete inverse materialized sampled', count() FROM t_ofsf_del_inv SAMPLE 0.5;
SELECT 'delete inverse materialized plain', count() FROM t_ofsf_del_inv;

DROP TABLE t_ofsf_del_inv;

-- A stored column over a shadowed physical _sample_factor keeps its inserted value under a pending
-- mutation.
DROP TABLE IF EXISTS t_ofsf_shadow;
CREATE TABLE t_ofsf_shadow (c0 UInt32, _sample_factor Float64, d Float64 DEFAULT _sample_factor * 3, u Float64 DEFAULT 0)
ENGINE = MergeTree ORDER BY c0 SAMPLE BY c0;
INSERT INTO t_ofsf_shadow (c0, _sample_factor) SELECT number, 42 FROM numbers(100);

SYSTEM STOP MERGES t_ofsf_shadow;
SET mutations_sync = 0;
ALTER TABLE t_ofsf_shadow UPDATE u = d WHERE 1;

SELECT 'pending', count() FROM system.mutations
WHERE database = currentDatabase() AND table = 't_ofsf_shadow' AND NOT is_done;

SELECT 'shadow preview sampled', any(u), any(_sample_factor) FROM t_ofsf_shadow SAMPLE 0.5 SETTINGS apply_mutations_on_fly = 1;

SYSTEM START MERGES t_ofsf_shadow;
SET mutations_sync = 2;
ALTER TABLE t_ofsf_shadow DELETE WHERE 0;

SELECT 'shadow materialized sampled', any(u), any(_sample_factor) FROM t_ofsf_shadow SAMPLE 0.5;

DROP TABLE t_ofsf_shadow;

-- A MATERIALIZED column recomputed by the mutation resolves its dependency to the shadowing physical
-- column, so `d` is 15 from the updated stored 5 and never 3 from a sampling factor.
DROP TABLE IF EXISTS t_ofsf_shadow_mat;
CREATE TABLE t_ofsf_shadow_mat (c0 UInt32, _sample_factor Float64, d Float64 MATERIALIZED _sample_factor * 3)
ENGINE = MergeTree ORDER BY c0 SAMPLE BY c0;
INSERT INTO t_ofsf_shadow_mat (c0, _sample_factor) SELECT number, 42 FROM numbers(100);

SYSTEM STOP MERGES t_ofsf_shadow_mat;
SET mutations_sync = 0;
ALTER TABLE t_ofsf_shadow_mat UPDATE _sample_factor = 5 WHERE 1;

SELECT 'pending', count() FROM system.mutations
WHERE database = currentDatabase() AND table = 't_ofsf_shadow_mat' AND NOT is_done;

SELECT 'shadow mat preview sampled', any(_sample_factor), any(d) FROM t_ofsf_shadow_mat SAMPLE 0.5 SETTINGS apply_mutations_on_fly = 1;
SELECT 'shadow mat preview plain', any(_sample_factor), any(d) FROM t_ofsf_shadow_mat SETTINGS apply_mutations_on_fly = 1;

SYSTEM START MERGES t_ofsf_shadow_mat;
SET mutations_sync = 2;
ALTER TABLE t_ofsf_shadow_mat DELETE WHERE 0;

SELECT 'shadow mat materialized sampled', any(_sample_factor), any(d) FROM t_ofsf_shadow_mat SAMPLE 0.5;

DROP TABLE t_ofsf_shadow_mat;
