-- An explicit reference t.key to a JOIN USING key whose merged supertype is Dynamic must be
-- typed with that supertype, not the table's own type. Otherwise an aggregate over it is built
-- from the wrong (Nullable) type while the planner produces a Dynamic column, and the Null
-- combinator aborts with "Bad cast from type DB::ColumnDynamic to DB::ColumnNullable".

SET enable_analyzer = 1;
SET allow_dynamic_type_in_join_keys = 1;

DROP TABLE IF EXISTS t_nt;
DROP TABLE IF EXISTS t_dyn;

CREATE TABLE t_nt (x Nullable(String)) ENGINE = Memory;
INSERT INTO t_nt VALUES ('id'), (NULL), ('1');

CREATE TABLE t_dyn (x Dynamic) ENGINE = Memory;
INSERT INTO t_dyn VALUES ('id'), (NULL), ('1');

-- The crashing query: count(...) IGNORE NULLS uses the Null combinator.
SELECT count(t1.x) IGNORE NULLS, sum(isNotNull(t1.x))
FROM t_nt AS t1 INNER JOIN t_dyn USING (x);

-- The explicit reference adopts the Dynamic supertype, matching the unqualified key.
SELECT toTypeName(t1.x) FROM t_nt AS t1 INNER JOIN t_dyn USING (x) LIMIT 1;
SELECT toTypeName(x) FROM t_nt AS t1 INNER JOIN t_dyn USING (x) LIMIT 1;

-- Plain count and LEFT JOIN must work too.
SELECT count(t1.x) FROM t_nt AS t1 INNER JOIN t_dyn USING (x);
SELECT count(t1.x) IGNORE NULLS FROM t_nt AS t1 LEFT JOIN t_dyn USING (x);

DROP TABLE t_nt;
DROP TABLE t_dyn;
