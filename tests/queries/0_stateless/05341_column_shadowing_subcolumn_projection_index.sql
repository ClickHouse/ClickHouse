-- A column `a.size0` added next to an `Array` column `a` shadows the sizes subcolumn of `a`,
-- so a projection that selects it must store the column itself, not compute the sizes of `a`.

DROP TABLE IF EXISTS t_shadow_projection_index;

CREATE TABLE t_shadow_projection_index (k UInt64, a Array(UInt64), PROJECTION p INDEX * TYPE basic())
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_shadow_projection_index VALUES (1, [1, 2, 3]);

ALTER TABLE t_shadow_projection_index ADD COLUMN `a.size0` Nullable(UInt64);

INSERT INTO t_shadow_projection_index VALUES (2, [1], 7);

SELECT k, a, `a.size0` FROM t_shadow_projection_index ORDER BY k;

OPTIMIZE TABLE t_shadow_projection_index FINAL;

SELECT k, a, `a.size0` FROM t_shadow_projection_index ORDER BY k;

DROP TABLE t_shadow_projection_index;

DROP TABLE IF EXISTS t_shadow_projection_normal;

CREATE TABLE t_shadow_projection_normal (k UInt64, a Array(UInt64)) ENGINE = MergeTree ORDER BY tuple();

ALTER TABLE t_shadow_projection_normal ADD COLUMN `a.size0` Nullable(UInt64);
ALTER TABLE t_shadow_projection_normal ADD PROJECTION p (SELECT k, `a.size0` ORDER BY `a.size0`);

INSERT INTO t_shadow_projection_normal VALUES (1, [1, 2, 3], 5), (2, [1], 7);

SELECT k, `a.size0` FROM t_shadow_projection_normal WHERE `a.size0` = 7 SETTINGS optimize_use_projections = 1;
SELECT k, `a.size0` FROM t_shadow_projection_normal WHERE `a.size0` = 7 SETTINGS optimize_use_projections = 0;

DROP TABLE t_shadow_projection_normal;
