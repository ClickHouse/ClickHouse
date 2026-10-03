-- The target table of a materialized view has fixed column types, but the `SELECT` of the view runs
-- with the settings of the `INSERT`. `LEFT ARRAY JOIN` under `array_join_use_nulls = 1` yields `Nullable`
-- columns, so the mismatch with a non-`Nullable` target must be reported, unless the view pins the setting.

DROP TABLE IF EXISTS mv_ajun;
DROP TABLE IF EXISTS mv_ajun_pinned;
DROP TABLE IF EXISTS mv_ajun_nullable;
DROP TABLE IF EXISTS src_ajun;
DROP TABLE IF EXISTS dst_ajun;
DROP TABLE IF EXISTS dst_ajun_pinned;
DROP TABLE IF EXISTS dst_ajun_nullable;

CREATE TABLE src_ajun (id UInt32, arr Array(UInt32)) ENGINE = MergeTree ORDER BY id;
CREATE TABLE dst_ajun (id UInt32, x UInt32) ENGINE = MergeTree ORDER BY id;
CREATE TABLE dst_ajun_pinned (id UInt32, x UInt32) ENGINE = MergeTree ORDER BY id;
CREATE TABLE dst_ajun_nullable (id UInt32, x Nullable(UInt32)) ENGINE = MergeTree ORDER BY id;

CREATE MATERIALIZED VIEW mv_ajun TO dst_ajun AS SELECT id, x FROM src_ajun LEFT ARRAY JOIN arr AS x;

INSERT INTO src_ajun SETTINGS array_join_use_nulls = 0 VALUES (1, [1, 2]), (2, []);
-- Reported even when no row of the block has an empty array.
INSERT INTO src_ajun SETTINGS array_join_use_nulls = 1 VALUES (3, [3]); -- { serverError INCORRECT_QUERY }
INSERT INTO src_ajun SETTINGS array_join_use_nulls = 1 VALUES (4, []); -- { serverError INCORRECT_QUERY }

DROP TABLE mv_ajun;

CREATE MATERIALIZED VIEW mv_ajun_pinned TO dst_ajun_pinned AS SELECT id, x FROM src_ajun LEFT ARRAY JOIN arr AS x SETTINGS array_join_use_nulls = 0;
CREATE MATERIALIZED VIEW mv_ajun_nullable TO dst_ajun_nullable AS SELECT id, x FROM src_ajun LEFT ARRAY JOIN arr AS x;

INSERT INTO src_ajun SETTINGS array_join_use_nulls = 1 VALUES (5, [5]), (6, []);

SELECT * FROM dst_ajun ORDER BY ALL;
SELECT '-';
SELECT * FROM dst_ajun_pinned ORDER BY ALL;
SELECT '-';
SELECT * FROM dst_ajun_nullable ORDER BY ALL;

DROP TABLE mv_ajun_pinned;
DROP TABLE mv_ajun_nullable;
DROP TABLE src_ajun;
DROP TABLE dst_ajun;
DROP TABLE dst_ajun_pinned;
DROP TABLE dst_ajun_nullable;
