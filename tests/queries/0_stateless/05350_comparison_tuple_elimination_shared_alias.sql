-- A nested single-element tuple comparison inside an alias shared by GROUP BY and ORDER BY / HAVING / WHERE
-- (identifier resolve cache) must be rewritten the same way as the projection copy.

DROP TABLE IF EXISTS t_tuple_elim;
DROP TABLE IF EXISTS t_tuple_elim_lc;
DROP TABLE IF EXISTS t_tuple_elim_l;
DROP TABLE IF EXISTS t_tuple_elim_r;
CREATE TABLE t_tuple_elim (s Nullable(String), n UInt64) ENGINE = MergeTree ORDER BY n;
INSERT INTO t_tuple_elim SELECT if(number = 5, NULL, toString(number % 3)), number FROM numbers(12);
CREATE TABLE t_tuple_elim_lc (s LowCardinality(String), n UInt64) ENGINE = MergeTree ORDER BY n;
INSERT INTO t_tuple_elim_lc SELECT toString(number % 3), number FROM numbers(12);
CREATE TABLE t_tuple_elim_l (key Nullable(UInt64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_tuple_elim_l VALUES (1), (NULL), (2);
CREATE TABLE t_tuple_elim_r (key Nullable(UInt64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_tuple_elim_r VALUES (1), (NULL), (3);

SET enable_identifier_resolve_cache = 1;

SELECT NOT (tuple(tuple(n, 1)) = tuple(tuple(n, 2))) AS r FROM t_tuple_elim GROUP BY r ORDER BY r;
SELECT NOT (tuple(tuple(n % 2, 1)) != tuple(tuple(1, 1))) AS r, count() FROM t_tuple_elim GROUP BY r ORDER BY r;
SELECT NOT (tuple(tuple(n % 2, 1)) = tuple((1, 1))) AS r, count() FROM t_tuple_elim GROUP BY r ORDER BY r;
SELECT NOT (tuple((1, 1)) = tuple(tuple(n % 2, 1))) AS r, count() FROM t_tuple_elim GROUP BY r HAVING r ORDER BY r;
SELECT NOT (tuple(tuple(tuple(n % 2, 1))) = tuple(tuple(tuple(1, 1)))) AS r, count() FROM t_tuple_elim WHERE NOT r GROUP BY r ORDER BY r;
SELECT NOT ((n % 2, tuple(tuple(n % 3, 1))) = (1, tuple(tuple(1, 1)))) AS r, count() FROM t_tuple_elim GROUP BY r ORDER BY r;
SELECT NOT (tuple(tuple(s, 1)) = tuple(tuple('1', 1))) AS r, count() FROM t_tuple_elim GROUP BY r ORDER BY r;
SELECT NOT (tuple(tuple(s, 1)) = tuple(tuple('1', 1))) AS r, count() FROM t_tuple_elim_lc GROUP BY r ORDER BY r;
SELECT NOT (tuple(tuple(n % 2, 1)) = tuple(tuple(1, 1))) AS r, count() FROM t_tuple_elim GROUP BY r WITH ROLLUP ORDER BY r, count();
SELECT NOT (tuple(tuple(n % 2, 1)) = tuple(tuple(1, 1))) AS r, count() FROM remote('127.0.0.{1,2}', currentDatabase(), t_tuple_elim) GROUP BY r ORDER BY r;

-- A comparison that a remote server would constant-fold is not created, so the remote header names match.
SELECT NOT (tuple(tuple(n, 1)) = tuple(tuple(n, 2))) AS r, min(s) FROM remote('127.0.0.{1,2}', currentDatabase(), t_tuple_elim) GROUP BY r ORDER BY r;
SELECT NOT (tuple(n, 1) = tuple(n, 2)) AS r, min(s) FROM remote('127.0.0.{1,2}', currentDatabase(), t_tuple_elim) GROUP BY r ORDER BY r;
SELECT tuple(n, s) = tuple(NULL, s) AS r FROM remote('127.0.0.{1,2}', currentDatabase(), t_tuple_elim) GROUP BY r ORDER BY r;
SELECT tuple(s) = tuple(CAST(NULL, 'Nullable(String)')) AS r, min(n) FROM remote('127.0.0.{1,2}', currentDatabase(), t_tuple_elim) GROUP BY r ORDER BY r;
SELECT tuple(s, n) = tuple((SELECT CAST(NULL, 'LowCardinality(Nullable(String))')), n) AS r FROM remote('127.0.0.{1,2}', currentDatabase(), t_tuple_elim) GROUP BY r ORDER BY r;
-- A comparison that a remote server does not fold into a constant depending on a column is still rewritten and uses the primary key.
SELECT n FROM t_tuple_elim WHERE (n, s) = ((SELECT toLowCardinality(toUInt64(4))), s) SETTINGS force_primary_key = 1;
SELECT n FROM t_tuple_elim WHERE (n, 1) = (4, 1) SETTINGS force_primary_key = 1;
SELECT n FROM t_tuple_elim WHERE (n, toLowCardinality(1)) = (4, toLowCardinality(2)) SETTINGS force_primary_key = 1;
SELECT n FROM t_tuple_elim WHERE (n, arrayJoin([1]), 1) = (4, 1, 2) SETTINGS force_primary_key = 1;
SELECT n FROM t_tuple_elim WHERE (n, materialize(toUInt64(1))) = (4, CAST(NULL, 'Nullable(UInt64)')) SETTINGS force_primary_key = 1;

-- The nested comparison is rewritten completely, so a join does not match the NULL keys, same as for tuple(l.key) = tuple(r.key).
SELECT count() FROM t_tuple_elim_l AS l CROSS JOIN t_tuple_elim_r AS r WHERE tuple(tuple(l.key)) = tuple(tuple(r.key));

DROP TABLE t_tuple_elim;
DROP TABLE t_tuple_elim_lc;
DROP TABLE t_tuple_elim_l;
DROP TABLE t_tuple_elim_r;
