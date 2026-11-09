-- Tags: no-replicated-database, no-parallel-replicas, no-random-merge-tree-settings
-- EXPLAIN output may differ

-- { echo }

DROP TABLE IF EXISTS test_simple_key;
CREATE TABLE test_simple_key (x Int32)
ENGINE = MergeTree ORDER BY tuple() PARTITION BY x
SETTINGS index_granularity = 1;

INSERT INTO test_simple_key SELECT number FROM numbers(15);

EXPLAIN indexes = 1
SELECT x FROM test_simple_key WHERE x + 2 = 9 ORDER BY x;

EXPLAIN indexes = 1
SELECT x FROM test_simple_key WHERE x + 2 > 9 ORDER BY x;

EXPLAIN indexes = 1
SELECT x FROM test_simple_key WHERE x + 2 < 9 ORDER BY x;

EXPLAIN indexes = 1
SELECT x FROM test_simple_key WHERE x + 2 != 9 ORDER BY x;


DROP TABLE IF EXISTS test_composite_key;
CREATE TABLE test_composite_key (x Int32)
ENGINE = MergeTree ORDER BY tuple() PARTITION BY x * x
SETTINGS index_granularity = 1;

INSERT INTO test_composite_key SELECT number FROM numbers(15);

EXPLAIN indexes = 1
SELECT x FROM test_composite_key WHERE x * x + 2 = 11 ORDER BY x;

EXPLAIN indexes = 1
SELECT x FROM test_composite_key WHERE x * x + 2 > 11 ORDER BY x;

EXPLAIN indexes = 1
SELECT x FROM test_composite_key WHERE x * x + 2 < 11 ORDER BY x;

EXPLAIN indexes = 1
SELECT x FROM test_composite_key WHERE x * x + 2 != 11 ORDER BY x;

DROP TABLE IF EXISTS test_composite_key2;
CREATE TABLE test_composite_key2 (x Int32)
ENGINE = MergeTree ORDER BY tuple() PARTITION BY if(x >= 0, -x, x)
SETTINGS index_granularity = 1;

INSERT INTO test_composite_key2 SELECT -number FROM numbers(10);
INSERT INTO test_composite_key2 SELECT number FROM numbers(10);

EXPLAIN indexes = 1
SELECT x FROM test_composite_key2 WHERE if(x >= 0, -x, x) + 2 = 1 ORDER BY x;

EXPLAIN indexes = 1
SELECT x FROM test_composite_key2 WHERE if(x >= 0, -x, x) + 2 > 1 ORDER BY x;

EXPLAIN indexes = 1
SELECT x FROM test_composite_key2 WHERE if(x >= 0, -x, x) + 2 < -1 ORDER BY x;

EXPLAIN indexes = 1
SELECT x FROM test_composite_key2 WHERE if(x >= 0, -x, x) + 2 != -1 ORDER BY x;


DROP TABLE IF EXISTS test_det_function;
CREATE TABLE test_det_function (x Int32)
ENGINE = MergeTree ORDER BY tuple() PARTITION BY x
SETTINGS index_granularity = 1;

INSERT INTO test_det_function SELECT number FROM numbers(15);

EXPLAIN indexes = 1
SELECT x FROM test_det_function WHERE cityHash64(x) % 5 = 3 ORDER BY x;

EXPLAIN indexes = 1
SELECT x FROM test_det_function WHERE cityHash64(x) % 5 > 3 ORDER BY x;

EXPLAIN indexes = 1
SELECT x FROM test_det_function WHERE cityHash64(x) % 5 < 3 ORDER BY x;

EXPLAIN indexes = 1
SELECT x FROM test_det_function WHERE cityHash64(x) % 5 != 3 ORDER BY x;

EXPLAIN indexes = 1
SELECT x FROM test_det_function WHERE cityHash64(x) % 5 != cityHash64(3) % 5 ORDER BY x;


DROP TABLE IF EXISTS test_strings;
CREATE TABLE test_strings (p String)
ENGINE = MergeTree ORDER BY tuple() PARTITION BY p
SETTINGS index_granularity = 1;

INSERT INTO test_strings VALUES ('abc'), ('def'), ('ghi'), ('jkl'), ('mnop'), ('q');

EXPLAIN indexes = 1
SELECT p FROM test_strings WHERE reverse(p) = 'cba' ORDER BY p;

EXPLAIN indexes = 1
SELECT p FROM test_strings WHERE hex(p) > '646566' ORDER BY p;

EXPLAIN indexes = 1
SELECT p FROM test_strings WHERE length(p) < 3 ORDER BY p;

EXPLAIN indexes = 1
SELECT p FROM test_strings WHERE length(p) != 3 ORDER BY p;

EXPLAIN indexes = 1
SELECT p FROM test_strings WHERE reverse(p) > 'lkj' ORDER BY p;

-- We only support deterministic function chain for now.
-- Even though it is mathematically correct for any arbitrary dag as well.
EXPLAIN indexes = 1
SELECT p FROM test_strings WHERE (left(p, length(p) - length(substringIndex(p, '-', -1)) - 1)) = 'abc';


DROP TABLE IF EXISTS test_monotonic_datetime;
CREATE TABLE test_monotonic_datetime (d Date, y String)
ENGINE = MergeTree ORDER BY tuple() PARTITION BY d
SETTINGS index_granularity = 1;

INSERT INTO test_monotonic_datetime VALUES ('2026-01-01', 'a'), ('2026-01-02', 'b'), ('2026-01-03', 'c');

SET date_time_overflow_behavior = 'ignore';
SET session_timezone = 'UTC';

EXPLAIN indexes = 1
SELECT d FROM test_monotonic_datetime WHERE toDateTime(d) = toDateTime('2026-01-02');


DROP TABLE IF EXISTS test_partition_key_to_year_month;
CREATE TABLE test_partition_key_to_year_month (d Date)
ENGINE = MergeTree ORDER BY tuple() PARTITION BY toYYYYMM(d)
SETTINGS index_granularity = 1;

INSERT INTO test_partition_key_to_year_month VALUES
    ('2024-01-15'),
    ('2024-02-15'),
    ('2024-03-15'),
    ('2024-04-15');

EXPLAIN indexes = 1
SELECT d
FROM test_partition_key_to_year_month
WHERE addMonths(toDate(concat(toString(toYYYYMM(d)), '01')), 1) > toDate('2024-03-01')
ORDER BY d;

EXPLAIN indexes = 1
SELECT d
FROM test_partition_key_to_year_month
WHERE addMonths(toDate(concat(toString(toYYYYMM(d)), '01')), 1) < toDate('2024-04-01')
ORDER BY d;

EXPLAIN indexes = 1
SELECT d
FROM test_partition_key_to_year_month
WHERE addMonths(toDate(concat(toString(toYYYYMM(d)), '01')), 1) != toDate('2024-03-01')
ORDER BY d;


DROP TABLE IF EXISTS test_nondeterministic_function;
CREATE TABLE test_nondeterministic_function (x Int32)
ENGINE = MergeTree ORDER BY tuple() PARTITION BY x
SETTINGS index_granularity = 1;

INSERT INTO test_nondeterministic_function SELECT number FROM numbers(15);

-- No atom created for partition pruning
EXPLAIN indexes = 1
SELECT count() FROM test_nondeterministic_function WHERE x + rand() = 9;
