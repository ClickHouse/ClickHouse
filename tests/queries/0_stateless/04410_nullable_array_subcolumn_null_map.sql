SET allow_experimental_nullable_array_type = 1;

CREATE TABLE t (a Nullable(Array(Tuple(x UInt8, y UInt16)))) ENGINE = Memory;

INSERT INTO t VALUES (NULL), ([]), ([(1, 10), (2, 20)]), ([(3, 30)]);

SELECT isNull(a.x) AS is_null, a.x AS val FROM t ORDER BY is_null, val;
SELECT isNull(a.y) AS is_null, a.y AS val FROM t ORDER BY is_null, val;

DROP TABLE t;
