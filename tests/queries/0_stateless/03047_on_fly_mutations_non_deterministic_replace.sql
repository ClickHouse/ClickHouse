DROP TABLE IF EXISTS t_lightweight_mut_5;

SET apply_mutations_on_fly = 1;
SET mutations_execute_subqueries_on_initiator = 1;
SET mutations_execute_nondeterministic_on_initiator = 1;

-- SELECT sum(...)

CREATE TABLE t_lightweight_mut_5 (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;

SYSTEM STOP MERGES t_lightweight_mut_5;
INSERT INTO t_lightweight_mut_5 VALUES (10, 20);

ALTER TABLE t_lightweight_mut_5 UPDATE v = (SELECT sum(number) FROM numbers(100)) WHERE 1;

SELECT id, v FROM t_lightweight_mut_5 ORDER BY id;

SELECT command FROM system.mutations
WHERE database = currentDatabase() AND table = 't_lightweight_mut_5' AND NOT is_done
ORDER BY command;

DROP TABLE t_lightweight_mut_5;

-- SELECT groupArray(...)

CREATE TABLE t_lightweight_mut_5 (id UInt64, v Array(UInt64)) ENGINE = MergeTree ORDER BY id;

SYSTEM STOP MERGES t_lightweight_mut_5;
INSERT INTO t_lightweight_mut_5 VALUES (10, [20]);

ALTER TABLE t_lightweight_mut_5 UPDATE v = (SELECT groupArray(number) FROM numbers(10)) WHERE 1;

SELECT id, v FROM t_lightweight_mut_5 ORDER BY id;

ALTER TABLE t_lightweight_mut_5 UPDATE v = (SELECT groupArray(number) FROM numbers(10000)) WHERE 1;

SELECT id, length(v) FROM t_lightweight_mut_5 ORDER BY id; -- { serverError BAD_ARGUMENTS }

SELECT command FROM system.mutations
WHERE database = currentDatabase() AND table = 't_lightweight_mut_5' AND NOT is_done
ORDER BY command;

SYSTEM START MERGES t_lightweight_mut_5;

-- Force to wait previous mutations
ALTER TABLE t_lightweight_mut_5 UPDATE v = v WHERE 1 SETTINGS mutations_sync = 2;

SELECT id, length(v) FROM t_lightweight_mut_5 ORDER BY id;

DROP TABLE t_lightweight_mut_5;

-- SELECT uniqExactState(...)

CREATE TABLE t_lightweight_mut_5 (id UInt64, v AggregateFunction(uniqExact, UInt64)) ENGINE = MergeTree ORDER BY id;

SYSTEM STOP MERGES t_lightweight_mut_5;
INSERT INTO t_lightweight_mut_5 VALUES (10, initializeAggregation('uniqExactState', 1::UInt64));

ALTER TABLE t_lightweight_mut_5 UPDATE v = (SELECT uniqExactState(number) FROM numbers(5)) WHERE 1;

SELECT id, finalizeAggregation(v) FROM t_lightweight_mut_5 ORDER BY id;

SELECT command FROM system.mutations
WHERE database = currentDatabase() AND table = 't_lightweight_mut_5' AND NOT is_done
ORDER BY command;

DROP TABLE t_lightweight_mut_5;

-- now()

CREATE TABLE t_lightweight_mut_5 (id UInt64, v DateTime) ENGINE = MergeTree ORDER BY id;

SYSTEM STOP MERGES t_lightweight_mut_5;
INSERT INTO t_lightweight_mut_5 VALUES (10, '2020-10-10');

ALTER TABLE t_lightweight_mut_5 UPDATE v = now() WHERE 1;

SELECT id, v BETWEEN now() - INTERVAL 10 MINUTE AND now() FROM t_lightweight_mut_5;

SELECT
    replaceRegexpOne(command, '(\\d{10})', 'timestamp'),
FROM system.mutations
WHERE database = currentDatabase() AND table = 't_lightweight_mut_5' AND NOT is_done
ORDER BY command;

DROP TABLE t_lightweight_mut_5;

-- filesystem(...)

CREATE TABLE t_lightweight_mut_5 (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;

SYSTEM STOP MERGES t_lightweight_mut_5;
INSERT INTO t_lightweight_mut_5 VALUES (10, 10);

ALTER TABLE t_lightweight_mut_5 UPDATE v = filesystemCapacity(materialize('default')) WHERE 1;

SELECT * FROM t_lightweight_mut_5 ORDER BY id; -- { serverError BAD_ARGUMENTS }
SELECT * FROM t_lightweight_mut_5 ORDER BY id SETTINGS apply_mutations_on_fly = 0;

SELECT command FROM system.mutations
WHERE database = currentDatabase() AND table = 't_lightweight_mut_5' AND NOT is_done
ORDER BY command;

DROP TABLE t_lightweight_mut_5;

-- UPDATE SELECT randConstant()

CREATE TABLE t_lightweight_mut_5 (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;

SYSTEM STOP MERGES t_lightweight_mut_5;
INSERT INTO t_lightweight_mut_5 VALUES (10, 10);

-- Check that function in subquery is not rewritten.
ALTER TABLE t_lightweight_mut_5
UPDATE v =
(
    SELECT sum(number) FROM numbers(1000) WHERE number > randConstant()
) WHERE 1 SETTINGS mutations_execute_subqueries_on_initiator = 0;

SELECT command FROM system.mutations
WHERE database = currentDatabase() AND table = 't_lightweight_mut_5' AND NOT is_done
ORDER BY command;

DROP TABLE t_lightweight_mut_5;

-- DELETE WHERE now()

CREATE TABLE t_lightweight_mut_5 (id UInt64, d DateTime) ENGINE = MergeTree ORDER BY id;

SYSTEM STOP MERGES t_lightweight_mut_5;
INSERT INTO t_lightweight_mut_5 VALUES (10, '2000-10-10'), (20, '2100-10-10');

ALTER TABLE t_lightweight_mut_5 DELETE WHERE d < now();

SELECT * FROM t_lightweight_mut_5 ORDER BY id SETTINGS apply_mutations_on_fly = 0;
SELECT * FROM t_lightweight_mut_5 ORDER BY id SETTINGS apply_mutations_on_fly = 1;

SELECT
    replaceRegexpOne(command, '(\\d{10})', 'timestamp'),
FROM system.mutations
WHERE database = currentDatabase() AND table = 't_lightweight_mut_5' AND NOT is_done
ORDER BY command;

DROP TABLE t_lightweight_mut_5;
