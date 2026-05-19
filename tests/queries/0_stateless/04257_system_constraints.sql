-- Test the system.constraints table: creation, ALTER ADD/DROP, and secret hiding.
DROP TABLE IF EXISTS test_constraints;

CREATE TABLE test_constraints
(
    a UInt32,
    b UInt32,
    CONSTRAINT check_a CHECK a > 0,
    CONSTRAINT assume_b ASSUME b < 1000
) ENGINE = MergeTree ORDER BY a;

SELECT name, type, expression
FROM system.constraints
WHERE database = currentDatabase() AND table = 'test_constraints'
ORDER BY name;

SELECT count(*)
FROM system.constraints
WHERE database = currentDatabase() AND table = 'test_constraints' AND type = 'CHECK';

SELECT count(*)
FROM system.constraints
WHERE database = currentDatabase() AND table = 'test_constraints' AND type = 'ASSUME';

ALTER TABLE test_constraints ADD CONSTRAINT check_b CHECK b > 0;

SELECT name, type, expression
FROM system.constraints
WHERE database = currentDatabase() AND table = 'test_constraints'
ORDER BY name;

ALTER TABLE test_constraints DROP CONSTRAINT check_a;

SELECT name, type, expression
FROM system.constraints
WHERE database = currentDatabase() AND table = 'test_constraints'
ORDER BY name;

ALTER TABLE test_constraints DROP CONSTRAINT assume_b;
ALTER TABLE test_constraints DROP CONSTRAINT check_b;

SELECT count(*)
FROM system.constraints
WHERE database = currentDatabase() AND table = 'test_constraints';

DROP TABLE test_constraints;

-- Test that secrets in constraint expressions are hidden
DROP TABLE IF EXISTS test_constraints_secrets;

CREATE TABLE test_constraints_secrets
(
    a String,
    CONSTRAINT check_enc CHECK length(encrypt('aes-128-ecb', a, 'supersecretkey')) > 0
) ENGINE = MergeTree ORDER BY a;

SELECT name, type, expression
FROM system.constraints
WHERE database = currentDatabase() AND table = 'test_constraints_secrets';

DROP TABLE test_constraints_secrets;

-- Test multiple tables: constraints don't bleed across tables
DROP TABLE IF EXISTS test_constraints_t1;
DROP TABLE IF EXISTS test_constraints_t2;

CREATE TABLE test_constraints_t1
(
    x UInt32,
    CONSTRAINT c1 CHECK x > 0
) ENGINE = MergeTree ORDER BY x;

CREATE TABLE test_constraints_t2
(
    y UInt32,
    CONSTRAINT c2 CHECK y < 100,
    CONSTRAINT c3 ASSUME y > 0
) ENGINE = MergeTree ORDER BY y;

SELECT table, name, type, expression
FROM system.constraints
WHERE database = currentDatabase() AND table IN ('test_constraints_t1', 'test_constraints_t2')
ORDER BY table, name;

DROP TABLE test_constraints_t1;
DROP TABLE test_constraints_t2;

-- Test non-MergeTree engine
DROP TABLE IF EXISTS test_constraints_memory;

CREATE TABLE test_constraints_memory
(
    a UInt32,
    CONSTRAINT check_mem CHECK a != 0
) ENGINE = Memory;

SELECT name, type, expression
FROM system.constraints
WHERE database = currentDatabase() AND table = 'test_constraints_memory';

DROP TABLE test_constraints_memory;

-- Test table with no constraints returns empty result
DROP TABLE IF EXISTS test_constraints_none;

CREATE TABLE test_constraints_none
(
    a UInt32
) ENGINE = MergeTree ORDER BY a;

SELECT count(*)
FROM system.constraints
WHERE database = currentDatabase() AND table = 'test_constraints_none';

DROP TABLE test_constraints_none;
