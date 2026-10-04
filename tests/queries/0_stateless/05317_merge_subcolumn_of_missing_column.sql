-- A subcolumn of a `Merge` column that a child table does not have is read from the value the column is
-- filled with: `x.null` of a missing `Nullable` column is 1, so `x IS NULL` holds on that child's rows.

DROP TABLE IF EXISTS t_05317_m;
DROP TABLE IF EXISTS t_05317_md;
DROP TABLE IF EXISTS t_05317_dist_b;
DROP TABLE IF EXISTS t_05317_a;
DROP TABLE IF EXISTS t_05317_b;
DROP TABLE IF EXISTS t_05317_c;

CREATE TABLE t_05317_a (id UInt64, x Nullable(UInt8), t Tuple(a Nullable(UInt8), b UInt8), v Variant(String, UInt64),
    `n.a` Array(UInt8), `n.b` Array(Nullable(UInt8))) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_05317_b (id UInt64, `n.a` Array(UInt8)) ENGINE = MergeTree ORDER BY id;
-- The ALIAS column `y` sends this child through the read path for children with ALIAS columns.
CREATE TABLE t_05317_c (id UInt64, y UInt64 ALIAS id + 1) ENGINE = MergeTree ORDER BY id;

INSERT INTO t_05317_a VALUES (1, 5, (5, 1), 's', [1], [7]), (2, NULL, (NULL, 2), NULL, [], []);
INSERT INTO t_05317_b VALUES (10, [1, 2]);
INSERT INTO t_05317_c VALUES (20);

CREATE TABLE t_05317_m (id UInt64, x Nullable(UInt8), t Tuple(a Nullable(UInt8), b UInt8), v Variant(String, UInt64),
    `n.a` Array(UInt8), `n.b` Array(Nullable(UInt8)), j JSON) ENGINE = Merge(currentDatabase(), '^t_05317_(a|b|c)$');

SELECT id, x, x.null, t.a.null, v.String.null, n.a, n.b, n.b.null, n.b.size0 FROM t_05317_m ORDER BY id;

SELECT count(x), countIf(x IS NULL), countIf(x IS NOT NULL), countIf(t.a IS NULL), countIf(isNull(v.String))
FROM t_05317_m SETTINGS optimize_functions_to_subcolumns = 1;
SELECT count(x), countIf(x IS NULL), countIf(x IS NOT NULL), countIf(t.a IS NULL), countIf(isNull(v.String))
FROM t_05317_m SETTINGS optimize_functions_to_subcolumns = 0;

SELECT id, n.a, length(n.b) FROM t_05317_m WHERE id = 10 SETTINGS optimize_functions_to_subcolumns = 1;
SELECT id, n.a, length(n.b) FROM t_05317_m WHERE id = 10 SETTINGS optimize_functions_to_subcolumns = 0;

SELECT count(x), countIf(x IS NULL) FROM merge(currentDatabase(), '^t_05317_(a|b)$') SETTINGS optimize_functions_to_subcolumns = 1;
SELECT count(x), countIf(x IS NULL) FROM merge(currentDatabase(), '^t_05317_(a|b)$') SETTINGS optimize_functions_to_subcolumns = 0;

-- No child has `j`.
SELECT id, j.a, j.a.:Int64.null FROM t_05317_m ORDER BY id;

-- A `Distributed` child runs the query itself, above `FetchColumns`.
CREATE TABLE t_05317_dist_b AS t_05317_b ENGINE = Distributed(test_shard_localhost, currentDatabase(), t_05317_b);
CREATE TABLE t_05317_md (id UInt64, x Nullable(UInt8), t Tuple(a Nullable(UInt8), b UInt8))
    ENGINE = Merge(currentDatabase(), '^t_05317_dist_b$');

SELECT sum(x.null), count() FROM merge(currentDatabase(), '^t_05317_(a|dist_b)$');
SELECT sum(t.a.null), count() FROM merge(currentDatabase(), '^t_05317_(a|dist_b)$');
SELECT sum(x.null), count() FROM t_05317_md;
SELECT sum(t.a.null), count() FROM t_05317_md;

DROP TABLE t_05317_m;
DROP TABLE t_05317_md;
DROP TABLE t_05317_dist_b;
DROP TABLE t_05317_a;
DROP TABLE t_05317_b;
DROP TABLE t_05317_c;
