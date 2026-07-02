-- Test: exercises `executeConst` in `arrayIndex.h` for `indexOf`/`countEqual`
-- with non-NULL constant array and Nullable value column carrying NULLs.
-- Covers: src/Functions/array/arrayIndex.h:1235 — `if (null_map && (*null_map)[row]) continue;`
-- Without the null_map check, the nested column's default value (e.g. 0) would
-- be compared to array elements, returning wrong results for NULL rows.
-- PR 61249 only covers `has`; `indexOf`/`countEqual` share the same shared path.

DROP TABLE IF EXISTS t_arr_idx_nullable;

CREATE TABLE t_arr_idx_nullable(a Nullable(UInt64)) ENGINE = Memory;
INSERT INTO t_arr_idx_nullable VALUES (0), (1), (NULL), (2);

SELECT 'indexOf with const non-NULL array, Nullable column';
SELECT a, indexOf([0, 1], a) FROM t_arr_idx_nullable ORDER BY a NULLS LAST;

SELECT 'countEqual with const non-NULL array, Nullable column';
SELECT a, countEqual([0, 1, 0, 1, 1], a) FROM t_arr_idx_nullable ORDER BY a NULLS LAST;

SELECT 'indexOf with Nullable(String)';
DROP TABLE t_arr_idx_nullable;
CREATE TABLE t_arr_idx_nullable(a Nullable(String)) ENGINE = Memory;
INSERT INTO t_arr_idx_nullable VALUES ('x'), (NULL), ('y');
SELECT a, indexOf(['x', 'y'], a), countEqual(['x', 'y', 'x'], a)
FROM t_arr_idx_nullable ORDER BY a NULLS LAST;

DROP TABLE t_arr_idx_nullable;
