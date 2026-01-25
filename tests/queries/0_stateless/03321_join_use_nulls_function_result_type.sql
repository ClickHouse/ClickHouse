-- Test for fix: function result type should be Nullable when arguments become Nullable due to join_use_nulls
-- This was causing "Unexpected return type from concat. Expected Nullable(String). Got String." error

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE t1 (id String, val Int32) ENGINE = Memory;
CREATE TABLE t2 (id String, name String) ENGINE = Memory;

INSERT INTO t1 VALUES ('a', 1), ('b', 2), ('c', 3);
INSERT INTO t2 VALUES ('a', 'Alice'), ('b', 'Bob');

SET join_use_nulls = 1;

-- t2.name becomes nullable due to LEFT JOIN with join_use_nulls
-- concat should return Nullable(String) because its argument is nullable
SELECT concat(t2.name, '_suffix') AS name_with_suffix, t1.val
FROM t1
LEFT JOIN t2 ON t1.id = t2.id
ORDER BY t1.val;

-- Verify the return type - should be Nullable(String)
SELECT toTypeName(concat(t2.name, '_suffix'))
FROM t1
LEFT JOIN t2 ON t1.id = t2.id
LIMIT 1;

-- Also test with upper() function
SELECT upper(t2.name) AS upper_name, t1.val
FROM t1
LEFT JOIN t2 ON t1.id = t2.id
ORDER BY t1.val;

SELECT toTypeName(upper(t2.name))
FROM t1
LEFT JOIN t2 ON t1.id = t2.id
LIMIT 1;

DROP TABLE t1;
DROP TABLE t2;
