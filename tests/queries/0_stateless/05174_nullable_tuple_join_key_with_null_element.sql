-- A `Nullable(Tuple(Nullable(T)))` join key that is not NULL at the top level but contains a NULL
-- element was treated as a NULL key by the hash-family algorithms and as an ordinary key by the merge
-- ones, so the row count of a join depended on `join_algorithm`. The null maps of the tuple's elements
-- were folded into the key's null map; only the top level says whether the key of a row is NULL.

SET enable_nullable_tuple_type = 1;

DROP TABLE IF EXISTS t_null_tuple_key;
CREATE TABLE t_null_tuple_key (t Nullable(Tuple(Nullable(Int64)))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_null_tuple_key VALUES (tuple(1)), (tuple(NULL)), (NULL);

SELECT 'every algorithm sees the same rows';
SELECT 'hash', count() FROM t_null_tuple_key AS a INNER JOIN t_null_tuple_key AS b ON a.t = b.t SETTINGS join_algorithm = 'hash';
SELECT 'parallel_hash', count() FROM t_null_tuple_key AS a INNER JOIN t_null_tuple_key AS b ON a.t = b.t SETTINGS join_algorithm = 'parallel_hash';
SELECT 'grace_hash', count() FROM t_null_tuple_key AS a INNER JOIN t_null_tuple_key AS b ON a.t = b.t SETTINGS join_algorithm = 'grace_hash';
SELECT 'full_sorting_merge', count() FROM t_null_tuple_key AS a INNER JOIN t_null_tuple_key AS b ON a.t = b.t SETTINGS join_algorithm = 'full_sorting_merge';
SELECT 'partial_merge', count() FROM t_null_tuple_key AS a INNER JOIN t_null_tuple_key AS b ON a.t = b.t SETTINGS join_algorithm = 'partial_merge';

SELECT 'a top-level Nullable wrap of a key that is never NULL changes nothing';
SELECT 'unwrapped', count() FROM (SELECT tuple(CAST(1, 'Nullable(Int64)')) AS t UNION ALL SELECT tuple(CAST(NULL, 'Nullable(Int64)'))) AS a
INNER JOIN (SELECT tuple(CAST(1, 'Nullable(Int64)')) AS t UNION ALL SELECT tuple(CAST(NULL, 'Nullable(Int64)'))) AS b ON a.t = b.t
SETTINGS join_algorithm = 'hash';
SELECT 'wrapped', count() FROM (SELECT tuple(CAST(1, 'Nullable(Int64)')) AS t UNION ALL SELECT tuple(CAST(NULL, 'Nullable(Int64)'))) AS a
INNER JOIN (SELECT tuple(CAST(1, 'Nullable(Int64)')) AS t UNION ALL SELECT tuple(CAST(NULL, 'Nullable(Int64)'))) AS b ON toNullable(a.t) = toNullable(b.t)
SETTINGS join_algorithm = 'hash';

SELECT 'a NULL key still joins nothing, and is not the same key as a tuple of NULL';
SELECT 'null key', count() FROM t_null_tuple_key AS a INNER JOIN t_null_tuple_key AS b ON a.t = b.t WHERE a.t IS NULL SETTINGS join_algorithm = 'hash';
SELECT 'in a set', t IN (SELECT tuple(CAST(NULL, 'Nullable(Int64)'))) FROM t_null_tuple_key ORDER BY isNull(t), t;

DROP TABLE t_null_tuple_key;
