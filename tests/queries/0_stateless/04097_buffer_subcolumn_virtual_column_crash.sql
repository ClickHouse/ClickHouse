-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/102104
-- Selecting subcolumns (e.g. .null, .size0, tuple elements, map keys/values)
-- through a Buffer table used to crash with:
--   "Logical error: Unknown virtual column: 'c0.null'"
-- because BufferSource didn't resolve subcolumns from the buffer's in-memory data.

DROP TABLE IF EXISTS t_buf_sub_dst;
DROP TABLE IF EXISTS t_buf_sub;

CREATE TABLE t_buf_sub_dst
(
    c0 Nullable(Int64),
    arr Array(Int32),
    tup Tuple(a Int32, b String),
    m Map(String, Int32)
)
ENGINE = MergeTree() ORDER BY tuple();

CREATE TABLE t_buf_sub ENGINE = Buffer(currentDatabase(), t_buf_sub_dst, 1, 86400, 86400, 1000000, 1000000, 1000000, 1000000);

-- Use large flush thresholds so data stays in the buffer
INSERT INTO t_buf_sub VALUES (1, [1,2,3], (10, 'hello'), {'k1': 100}), (NULL, [4,5], (20, 'world'), {'k2': 200});

-- Nullable subcolumn
SELECT c0.null FROM t_buf_sub ORDER BY c0.null;

-- Array size subcolumn
SELECT arr.size0 FROM t_buf_sub ORDER BY arr.size0;

-- Tuple element subcolumns
SELECT tup.a, tup.b FROM t_buf_sub ORDER BY tup.a;

-- Map keys/values subcolumns
SELECT m.keys, m.values FROM t_buf_sub ORDER BY m.keys;

-- Virtual column should still work
SELECT _table FROM t_buf_sub ORDER BY _table LIMIT 1;

DROP TABLE t_buf_sub;
DROP TABLE t_buf_sub_dst;
