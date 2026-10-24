-- A column `a.size0` declared next to an `Array` column `a` and omitted from a part must not be
-- matched by the sizes subcolumn of `a` when deciding whether `MODIFY COLUMN` can skip the mutation.

DROP TABLE IF EXISTS t_shadow_fast_path;

CREATE TABLE t_shadow_fast_path (k UInt64, a Array(UInt64), `a.size0` UInt64)
ENGINE = MergeTree ORDER BY k
SETTINGS skip_empty_columns_on_insert = 1, serialization_info_version = 'with_missing_columns', min_bytes_for_wide_part = 0;

INSERT INTO t_shadow_fast_path (k, a) VALUES (1, [1, 2, 3]);

SELECT column, type FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_shadow_fast_path' AND active AND column NOT LIKE '\_%' ORDER BY column;

ALTER TABLE t_shadow_fast_path MODIFY COLUMN `a.size0` Nullable(UInt64) SETTINGS mutations_sync = 2;

SELECT column, type FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_shadow_fast_path' AND active AND column NOT LIKE '\_%' ORDER BY column;
SELECT k, a, `a.size0`, toTypeName(`a.size0`) FROM t_shadow_fast_path;

DROP TABLE t_shadow_fast_path;
