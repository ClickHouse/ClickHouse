SET optimize_trivial_insert_select = 1;

DROP TABLE IF EXISTS t_sparse;

CREATE TABLE t_sparse (id UInt64, u UInt64, s String)
ENGINE = MergeTree ORDER BY id
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.9, index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO t_sparse SELECT
    number,
    if (number % 20 = 0, number, 0),
    if (number % 50 = 0, toString(number), '')
FROM numbers(1, 100000);

SELECT column, serialization_kind FROM system.parts_columns
WHERE table = 't_sparse' AND database = currentDatabase()
ORDER BY column, serialization_kind;

SELECT count() FROM t_sparse WHERE u > 0;
SELECT count() FROM t_sparse WHERE notEmpty(s);

SYSTEM STOP MERGES t_sparse;

INSERT INTO t_sparse SELECT
    number, number, toString(number)
FROM numbers (1, 100000);

SELECT column, serialization_kind FROM system.parts_columns
WHERE table = 't_sparse' AND database = currentDatabase()
ORDER BY column, serialization_kind;

SELECT count() FROM t_sparse WHERE u > 0;
SELECT count() FROM t_sparse WHERE notEmpty(s);

DROP TABLE t_sparse;

-- `ColumnSparse::index` with sorted, partially sorted and repeated indexes must match a table without sparse columns.

DROP TABLE IF EXISTS t_sparse_index;
DROP TABLE IF EXISTS t_full_index;

CREATE TABLE t_sparse_index (id UInt64, a Int16, b UInt64, c UInt32, s String)
ENGINE = MergeTree ORDER BY id
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.5, index_granularity = 8192;

CREATE TABLE t_full_index AS t_sparse_index
ENGINE = MergeTree ORDER BY id
SETTINGS ratio_of_defaults_for_sparse_serialization = 1, index_granularity = 8192;

INSERT INTO t_sparse_index SELECT
    number,
    cityHash64(number) % 100 = 0 ? 1 + number % 7 : 0,
    intHash32(number) % 10 < 3 ? number : 0,
    intHash64(number) % 1000 = 0 ? number : 0,
    cityHash64(number, 1) % 20 = 0 ? repeat('x', number % 30) || toString(number) : ''
FROM numbers(300000);

INSERT INTO t_full_index SELECT * FROM t_sparse_index;

SELECT table, column, serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table IN ('t_sparse_index', 't_full_index') AND active AND column != 'id'
GROUP BY ALL ORDER BY ALL;

SET optimize_move_to_prewhere = 0, enable_lazy_columns_replication = 1;

-- Filter by the sparse filter's own offsets.
SELECT (SELECT groupArray((a, n)) FROM (SELECT a, count() AS n FROM t_sparse_index WHERE a != 0 GROUP BY a ORDER BY a))
     = (SELECT groupArray((a, n)) FROM (SELECT a, count() AS n FROM t_full_index WHERE a != 0 GROUP BY a ORDER BY a));

-- Index other sparse columns by the filter's offsets: a denser one (`b`), a sparser one (`c`) and a `String` one (`s`).
SELECT (SELECT (sum(b), sum(c), sum(cityHash64(s)), max(s)) FROM t_sparse_index WHERE a != 0)
     = (SELECT (sum(b), sum(c), sum(cityHash64(s)), max(s)) FROM t_full_index WHERE a != 0);

SELECT (SELECT (sum(a), sum(b), sum(cityHash64(s))) FROM t_sparse_index WHERE c != 0)
     = (SELECT (sum(a), sum(b), sum(cityHash64(s))) FROM t_full_index WHERE c != 0);

-- Sorting lazily replicated rows by `(x, id)`: the indexes ascend for `x = 1`, then go back for `x = 2`.
SELECT (SELECT groupArray((a, b, c, s)) FROM (SELECT a, b, c, s FROM t_sparse_index ARRAY JOIN if(cityHash64(id, 2) % 50 = 0, [1, 2, 3], []) AS x ORDER BY x, id))
     = (SELECT groupArray((a, b, c, s)) FROM (SELECT a, b, c, s FROM t_full_index ARRAY JOIN if(cityHash64(id, 2) % 50 = 0, [1, 2, 3], []) AS x ORDER BY x, id));

-- Lazy `ARRAY JOIN` repeats a few rows of each block.
SELECT (SELECT (sum(a), sum(b), sum(c), max(s)) FROM t_sparse_index ARRAY JOIN if(cityHash64(id, 2) % 50 = 0, [1, 2, 3], []) AS x)
     = (SELECT (sum(a), sum(b), sum(c), max(s)) FROM t_full_index ARRAY JOIN if(cityHash64(id, 2) % 50 = 0, [1, 2, 3], []) AS x);

DROP TABLE t_sparse_index;
DROP TABLE t_full_index;
