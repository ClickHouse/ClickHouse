-- A column `a.size0` added next to an `Array` column `a` is missing from a projection written before,
-- so the projection must not be used to read it as if it held the sizes of `a`.

DROP TABLE IF EXISTS t_shadow_projection;

CREATE TABLE t_shadow_projection (k UInt64, a Array(UInt64), PROJECTION p (SELECT k, a ORDER BY k))
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_shadow_projection VALUES (1, [1, 2, 3]);

ALTER TABLE t_shadow_projection ADD COLUMN `a.size0` UInt64;

INSERT INTO t_shadow_projection VALUES (2, [1], 7);

SELECT k, `a.size0` FROM t_shadow_projection WHERE k = 1 SETTINGS optimize_use_projections = 1;
SELECT k, `a.size0` FROM t_shadow_projection WHERE k IN (1, 2) ORDER BY k SETTINGS optimize_use_projections = 1;
SELECT k, `a.size0` FROM t_shadow_projection WHERE k IN (1, 2) ORDER BY k SETTINGS optimize_use_projections = 0;

OPTIMIZE TABLE t_shadow_projection FINAL;

SELECT k, `a.size0` FROM t_shadow_projection WHERE k IN (1, 2) ORDER BY k SETTINGS optimize_use_projections = 1;

DROP TABLE t_shadow_projection;
