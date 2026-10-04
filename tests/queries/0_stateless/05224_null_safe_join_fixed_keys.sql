-- A null-safe comparison (`<=>`) of nullable keys is joined through two plain keys, `isNull(x)` and
-- `if(isNull(x), default, assumeNotNull(x))`, instead of a serialized `tuple(x)`.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_ns_left;
DROP TABLE IF EXISTS t_ns_right;
CREATE TABLE t_ns_left (id UInt64, a Nullable(UInt64), b Nullable(String), c LowCardinality(Nullable(String)), d Nullable(Date)) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_ns_right (id UInt64, a Nullable(UInt64), b Nullable(String), c LowCardinality(Nullable(String)), d Nullable(Date)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_ns_left SELECT number, if(number % 3 = 0, NULL, number % 7), if(number % 5 = 0, NULL, toString(number % 4)), if(number % 4 = 0, NULL, toString(number % 3)), if(number % 6 = 0, NULL, toDate('2026-01-01') + number % 5) FROM numbers(200);
INSERT INTO t_ns_right SELECT number, if(number % 4 = 0, NULL, number % 7), if(number % 3 = 0, NULL, toString(number % 4)), if(number % 5 = 0, NULL, toString(number % 3)), if(number % 7 = 0, NULL, toDate('2026-01-01') + number % 5) FROM numbers(200);

SELECT 'the keys';
SELECT trim(explain) FROM (EXPLAIN actions = 1 SELECT l.id FROM t_ns_left l JOIN t_ns_right r ON l.a <=> r.a) WHERE explain LIKE '%Join conditions%';
SELECT trim(explain) FROM (EXPLAIN actions = 1 SELECT l.id FROM t_ns_left l JOIN t_ns_right r ON l.a <=> r.a AND l.b <=> r.b AND l.c <=> r.c AND l.d <=> r.d) WHERE explain LIKE '%Join conditions%';
SELECT trim(explain) FROM (EXPLAIN actions = 1 SELECT l.id FROM t_ns_left l JOIN t_ns_right r ON l.a <=> r.id) WHERE explain LIKE '%Join conditions%';

SELECT 'NULL matches NULL and nothing else, for every kind';
SELECT count(), countIf(l.a IS NULL), sum(l.id + r.id) FROM t_ns_left l INNER JOIN t_ns_right r ON l.a <=> r.a AND l.b <=> r.b;
SELECT count(), countIf(l.a IS NULL), sum(l.id + r.id) FROM t_ns_left l INNER JOIN t_ns_right r ON tuple(l.a) = tuple(r.a) AND tuple(l.b) = tuple(r.b);
SELECT count(), countIf(r.id = 0), sum(l.id + r.id) FROM t_ns_left l LEFT JOIN t_ns_right r ON l.a <=> r.a AND l.c <=> r.c AND l.d <=> r.d;
SELECT count(), countIf(r.id = 0), sum(l.id + r.id) FROM t_ns_left l LEFT JOIN t_ns_right r ON tuple(l.a) = tuple(r.a) AND tuple(l.c) = tuple(r.c) AND tuple(l.d) = tuple(r.d);
SELECT count(), countIf(l.id = 0), sum(l.id + r.id) FROM t_ns_left l RIGHT JOIN t_ns_right r ON l.a <=> r.a AND l.b <=> r.b AND l.c <=> r.c;
SELECT count(), countIf(l.id = 0), sum(l.id + r.id) FROM t_ns_left l RIGHT JOIN t_ns_right r ON tuple(l.a) = tuple(r.a) AND tuple(l.b) = tuple(r.b) AND tuple(l.c) = tuple(r.c);
SELECT count(), countIf(l.id = 0), countIf(r.id = 0) FROM t_ns_left l FULL JOIN t_ns_right r ON l.a <=> r.a AND l.d <=> r.d;
SELECT count(), countIf(l.id = 0), countIf(r.id = 0) FROM t_ns_left l FULL JOIN t_ns_right r ON tuple(l.a) = tuple(r.a) AND tuple(l.d) = tuple(r.d);
SELECT count() FROM t_ns_left l SEMI LEFT JOIN t_ns_right r ON l.b <=> r.b AND l.c <=> r.c;
SELECT count() FROM t_ns_left l ANTI LEFT JOIN t_ns_right r ON l.b <=> r.b AND l.c <=> r.c;
SELECT count() FROM t_ns_left l ANY LEFT JOIN t_ns_right r ON l.a <=> r.a WHERE r.id > 0;

SELECT 'the right key column is the stored one, not the derived key';
SELECT l.a, r.a, r.b FROM t_ns_left l JOIN t_ns_right r ON l.a <=> r.a AND l.b <=> r.b WHERE l.id < 4 AND r.id < 4 ORDER BY l.id, r.id;
SELECT l.a, r.a FROM t_ns_left l LEFT JOIN t_ns_right r ON l.a <=> r.a AND l.b <=> r.b WHERE l.id < 3 AND r.id < 3 ORDER BY l.id, r.id SETTINGS join_use_nulls = 1;

SELECT 'one nullable side';
SELECT count(), sum(l.id) FROM t_ns_left l JOIN t_ns_right r ON l.a <=> r.id;
SELECT count(), sum(l.id) FROM t_ns_left l JOIN t_ns_right r ON tuple(l.a) = tuple(toNullable(r.id));

SELECT 'every algorithm';
SELECT count(), sum(l.id + r.id) FROM t_ns_left l JOIN t_ns_right r ON l.a <=> r.a AND l.b <=> r.b SETTINGS join_algorithm = 'hash';
SELECT count(), sum(l.id + r.id) FROM t_ns_left l JOIN t_ns_right r ON l.a <=> r.a AND l.b <=> r.b SETTINGS join_algorithm = 'parallel_hash';
SELECT count(), sum(l.id + r.id) FROM t_ns_left l JOIN t_ns_right r ON l.a <=> r.a AND l.b <=> r.b SETTINGS join_algorithm = 'grace_hash';
SELECT count(), sum(l.id + r.id) FROM t_ns_left l JOIN t_ns_right r ON l.a <=> r.a AND l.b <=> r.b SETTINGS join_algorithm = 'full_sorting_merge';
SELECT count(), sum(l.id + r.id) FROM t_ns_left l JOIN t_ns_right r ON l.a <=> r.a AND l.b <=> r.b SETTINGS join_algorithm = 'partial_merge';

SELECT 'compound and Nothing keys keep the tuple';
SELECT * FROM (SELECT NULL AS x) l JOIN (SELECT NULL AS x) r ON l.x <=> r.x;
SELECT trim(explain) FROM (EXPLAIN actions = 1 SELECT * FROM (SELECT NULL AS x) l JOIN (SELECT NULL AS x) r ON l.x <=> r.x) WHERE explain LIKE '%Join conditions%';
WITH t AS (SELECT if(number % 3 = 0, NULL, tuple(if(number % 5, NULL, number))) AS k FROM numbers(4)) SELECT count(), countIf(isNull(l.k) AND isNull(r.k)) FROM t l JOIN t r ON l.k <=> r.k;
WITH t AS (SELECT if(number = 0, NULL, tuple(NULL)) AS k FROM numbers(2)) SELECT count() FROM t l JOIN t r ON l.k <=> r.k;
SELECT trim(explain) FROM (EXPLAIN actions = 1 WITH t AS (SELECT if(number % 3 = 0, NULL, tuple(if(number % 5, NULL, number))) AS k FROM numbers(4)) SELECT count() FROM t l JOIN t r ON l.k <=> r.k) WHERE explain LIKE '%Join conditions%';

DROP TABLE t_ns_left;
DROP TABLE t_ns_right;
