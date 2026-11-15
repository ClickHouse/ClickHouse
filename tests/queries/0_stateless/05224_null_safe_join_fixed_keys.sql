-- A null-safe comparison (`<=>`) of nullable keys is joined through two plain keys, `if(isNull(x), default, assumeNotNull(x))`
-- and `isNull(x)`, instead of a serialized `tuple(x)`, if all keys of the clause still pack into the 32 bytes of a
-- fixed-width hash key. The value key goes first, so that merge joins can skip runs of rows by it.

SET query_plan_optimize_join_order_randomize = 0; -- Pinned because the test asserts on join plan/order
SET query_plan_join_swap_table = 0;
SET allow_suspicious_low_cardinality_types = 1;

DROP TABLE IF EXISTS t_ns_left;
DROP TABLE IF EXISTS t_ns_right;
CREATE TABLE t_ns_left (id UInt64, a Nullable(UInt64), b Nullable(String), c LowCardinality(Nullable(String)), d Nullable(Date)) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_ns_right (id UInt64, a Nullable(UInt64), b Nullable(String), c LowCardinality(Nullable(String)), d Nullable(Date)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_ns_left SELECT number, if(number % 3 = 0, NULL, number % 7), if(number % 5 = 0, NULL, toString(number % 4)), if(number % 4 = 0, NULL, toString(number % 3)), if(number % 6 = 0, NULL, toDate('2026-01-01') + number % 5) FROM numbers(200);
INSERT INTO t_ns_right SELECT number, if(number % 4 = 0, NULL, number % 7), if(number % 3 = 0, NULL, toString(number % 4)), if(number % 5 = 0, NULL, toString(number % 3)), if(number % 7 = 0, NULL, toDate('2026-01-01') + number % 5) FROM numbers(200);

SELECT 'the keys';
SELECT extract(explain, 'Join conditions: .*') FROM (EXPLAIN actions = 1 SELECT l.id FROM t_ns_left l JOIN t_ns_right r ON l.a <=> r.a) WHERE explain LIKE '%Join conditions%';
SELECT extract(explain, 'Join conditions: .*') FROM (EXPLAIN actions = 1 SELECT l.id FROM t_ns_left l JOIN t_ns_right r ON l.a <=> r.a AND l.b <=> r.b AND l.c <=> r.c AND l.d <=> r.d) WHERE explain LIKE '%Join conditions%';
SELECT extract(explain, 'Join conditions: .*') FROM (EXPLAIN actions = 1 SELECT l.id FROM t_ns_left l JOIN t_ns_right r ON l.a <=> r.id) WHERE explain LIKE '%Join conditions%';
SELECT extract(explain, 'Join conditions: .*') FROM (EXPLAIN actions = 1 SELECT l.id FROM t_ns_left l JOIN t_ns_right r ON l.b::Nullable(FixedString(1)) <=> r.b::Nullable(FixedString(1))) WHERE explain LIKE '%Join conditions%';

SELECT 'NULL matches NULL and nothing else, for every kind';
SELECT count(), countIf(l.a IS NULL), sum(l.id + r.id) FROM t_ns_left l INNER JOIN t_ns_right r ON l.a <=> r.a AND l.d <=> r.d;
SELECT count(), countIf(l.a IS NULL), sum(l.id + r.id) FROM t_ns_left l INNER JOIN t_ns_right r ON tuple(l.a) = tuple(r.a) AND tuple(l.d) = tuple(r.d);
SELECT count(), countIf(r.id = 0), sum(l.id + r.id) FROM t_ns_left l LEFT JOIN t_ns_right r ON l.a <=> r.a AND l.c <=> r.c AND l.d <=> r.d;
SELECT count(), countIf(r.id = 0), sum(l.id + r.id) FROM t_ns_left l LEFT JOIN t_ns_right r ON tuple(l.a) = tuple(r.a) AND tuple(l.c) = tuple(r.c) AND tuple(l.d) = tuple(r.d);
SELECT count(), countIf(l.id = 0), sum(l.id + r.id) FROM t_ns_left l RIGHT JOIN t_ns_right r ON l.a <=> r.a AND l.b <=> r.b AND l.c <=> r.c;
SELECT count(), countIf(l.id = 0), sum(l.id + r.id) FROM t_ns_left l RIGHT JOIN t_ns_right r ON tuple(l.a) = tuple(r.a) AND tuple(l.b) = tuple(r.b) AND tuple(l.c) = tuple(r.c);
SELECT count(), countIf(l.id = 0), countIf(r.id = 0) FROM t_ns_left l FULL JOIN t_ns_right r ON l.a <=> r.a AND l.d <=> r.d;
SELECT count(), countIf(l.id = 0), countIf(r.id = 0) FROM t_ns_left l FULL JOIN t_ns_right r ON tuple(l.a) = tuple(r.a) AND tuple(l.d) = tuple(r.d);
SELECT count(), countIf(l.b IS NULL), sum(l.id + r.id) FROM t_ns_left l INNER JOIN t_ns_right r ON l.a <=> r.a AND l.b::Nullable(FixedString(1)) <=> r.b::Nullable(FixedString(1));
SELECT count(), countIf(l.b IS NULL), sum(l.id + r.id) FROM t_ns_left l INNER JOIN t_ns_right r ON tuple(l.a) = tuple(r.a) AND tuple(l.b::Nullable(FixedString(1))) = tuple(r.b::Nullable(FixedString(1)));
SELECT count() FROM t_ns_left l SEMI LEFT JOIN t_ns_right r ON l.b <=> r.b AND l.c <=> r.c;
SELECT count() FROM t_ns_left l ANTI LEFT JOIN t_ns_right r ON l.b <=> r.b AND l.c <=> r.c;
SELECT count(), countIf(r.id IS NOT NULL), countIf(l.a <=> r.a) FROM t_ns_left l ANY LEFT JOIN t_ns_right r ON l.a <=> r.a SETTINGS join_use_nulls = 1;

SELECT 'the right key column is the stored one, not the derived key';
SELECT l.a, r.a, r.d FROM t_ns_left l JOIN t_ns_right r ON l.a <=> r.a AND l.d <=> r.d WHERE l.id < 4 AND r.id < 4 ORDER BY l.id, r.id;
SELECT l.a, r.a FROM t_ns_left l LEFT JOIN t_ns_right r ON l.a <=> r.a AND l.d <=> r.d WHERE l.id < 3 AND r.id < 3 ORDER BY l.id, r.id SETTINGS join_use_nulls = 1;

SELECT 'one nullable side';
SELECT count(), sum(l.id) FROM t_ns_left l JOIN t_ns_right r ON l.a <=> r.id;
SELECT count(), sum(l.id) FROM t_ns_left l JOIN t_ns_right r ON tuple(l.a) = tuple(toNullable(r.id));

SELECT 'every algorithm';
SELECT count(), sum(l.id + r.id) FROM t_ns_left l JOIN t_ns_right r ON l.a <=> r.a AND l.d <=> r.d SETTINGS join_algorithm = 'hash';
SELECT count(), sum(l.id + r.id) FROM t_ns_left l JOIN t_ns_right r ON l.a <=> r.a AND l.d <=> r.d SETTINGS join_algorithm = 'parallel_hash';
SELECT count(), sum(l.id + r.id) FROM t_ns_left l JOIN t_ns_right r ON l.a <=> r.a AND l.d <=> r.d SETTINGS join_algorithm = 'grace_hash';
SELECT count(), sum(l.id + r.id) FROM t_ns_left l JOIN t_ns_right r ON l.a <=> r.a AND l.d <=> r.d SETTINGS join_algorithm = 'full_sorting_merge';
SELECT count(), sum(l.id + r.id) FROM t_ns_left l JOIN t_ns_right r ON l.a <=> r.a AND l.d <=> r.d SETTINGS join_algorithm = 'partial_merge';
SELECT count(), sum(l.id + r.id) FROM t_ns_left l ASOF JOIN t_ns_right r ON l.a <=> r.a AND l.id >= r.id SETTINGS join_algorithm = 'hash';
SELECT count(), sum(l.id + r.id) FROM t_ns_left l ASOF JOIN t_ns_right r ON l.a <=> r.a AND l.id >= r.id SETTINGS join_algorithm = 'full_sorting_merge';

SELECT 'String, compound and Nothing keys keep the tuple';
SELECT * FROM (SELECT NULL AS x) l JOIN (SELECT NULL AS x) r ON l.x <=> r.x;
SELECT extract(explain, 'Join conditions: .*') FROM (EXPLAIN actions = 1 SELECT * FROM (SELECT NULL AS x) l JOIN (SELECT NULL AS x) r ON l.x <=> r.x) WHERE explain LIKE '%Join conditions%';
WITH t AS (SELECT if(number % 3 = 0, NULL, tuple(if(number % 5, NULL, number))) AS k FROM numbers(4)) SELECT count(), countIf(isNull(l.k) AND isNull(r.k)) FROM t l JOIN t r ON l.k <=> r.k;
WITH t AS (SELECT if(number % 3 = 0, NULL, tuple(if(number % 5, NULL, number))) AS k FROM numbers(4)) SELECT count(), countIf(isNull(l.k) AND isNull(r.k)) FROM t l JOIN t r ON tuple(l.k) = tuple(r.k);
WITH t AS (SELECT if(number = 0, NULL, tuple(NULL)) AS k FROM numbers(2)) SELECT count() FROM t l JOIN t r ON l.k <=> r.k;
SELECT extract(explain, 'Join conditions: .*') FROM (EXPLAIN actions = 1 WITH t AS (SELECT if(number % 3 = 0, NULL, tuple(if(number % 5, NULL, number))) AS k FROM numbers(4)) SELECT count() FROM t l JOIN t r ON l.k <=> r.k) WHERE explain LIKE '%Join conditions%';

SELECT 'the keys split only if the whole clause packs into 32 bytes';
SELECT extract(explain, 'Join conditions: .*') FROM (EXPLAIN actions = 1 SELECT l.id FROM t_ns_left l JOIN t_ns_right r ON l.a <=> r.a AND l.a + 1 <=> r.a + 1 AND l.a + 2 <=> r.a + 2) WHERE explain LIKE '%Join conditions%';
SELECT extract(explain, 'Join conditions: .*') FROM (EXPLAIN actions = 1 SELECT l.id FROM t_ns_left l JOIN t_ns_right r ON l.a <=> r.a AND l.a + 1 <=> r.a + 1 AND l.a + 2 <=> r.a + 2 AND l.a + 3 <=> r.a + 3) WHERE explain LIKE '%Join conditions%';
SELECT extract(explain, 'Join conditions: .*') FROM (EXPLAIN actions = 1 SELECT l.id FROM t_ns_left l JOIN t_ns_right r ON l.a::Nullable(UInt256) <=> r.a::Nullable(UInt256)) WHERE explain LIKE '%Join conditions%';
SELECT extract(explain, 'Join conditions: .*') FROM (EXPLAIN actions = 1 SELECT l.id FROM t_ns_left l JOIN t_ns_right r ON l.a <=> r.a AND l.id = r.id AND l.b = r.b) WHERE explain LIKE '%Join conditions%';
SELECT count(), countIf(l.a IS NULL), sum(l.id + r.id) FROM t_ns_left l INNER JOIN t_ns_right r ON l.a::Nullable(UInt256) <=> r.a::Nullable(UInt256) AND l.d <=> r.d;
SELECT extract(explain, 'Join conditions: .*') FROM (EXPLAIN actions = 1 SELECT l.id FROM t_ns_left l JOIN t_ns_right r ON l.a <=> r.a AND toLowCardinality(l.id) = toLowCardinality(r.id)) WHERE explain LIKE '%Join conditions%';

SELECT 'LowCardinality, UUID and IPv6 keys';
SELECT extract(explain, 'Join conditions: .*') FROM (EXPLAIN actions = 1 SELECT l.id FROM t_ns_left l JOIN t_ns_right r ON l.a::LowCardinality(Nullable(UInt64)) <=> r.a::LowCardinality(Nullable(UInt64))) WHERE explain LIKE '%Join conditions%';
SELECT count(), countIf(l.a IS NULL), sum(l.id + r.id) FROM t_ns_left l INNER JOIN t_ns_right r ON l.a::LowCardinality(Nullable(UInt64)) <=> r.a::LowCardinality(Nullable(UInt64));
SELECT count(), countIf(l.a IS NULL), sum(l.id + r.id) FROM t_ns_left l INNER JOIN t_ns_right r ON tuple(l.a) = tuple(r.a);
SELECT extract(explain, 'Join conditions: .*') FROM (EXPLAIN actions = 1 SELECT l.id FROM t_ns_left l JOIN t_ns_right r ON if(l.a IS NULL, NULL, reinterpretAsUUID(toUInt128(l.a)))::Nullable(UUID) <=> if(r.a IS NULL, NULL, reinterpretAsUUID(toUInt128(r.a)))::Nullable(UUID) AND l.d <=> r.d) WHERE explain LIKE '%Join conditions%';
SELECT count(), countIf(l.a IS NULL), sum(l.id + r.id) FROM t_ns_left l INNER JOIN t_ns_right r ON if(l.a IS NULL, NULL, reinterpretAsUUID(toUInt128(l.a)))::Nullable(UUID) <=> if(r.a IS NULL, NULL, reinterpretAsUUID(toUInt128(r.a)))::Nullable(UUID) AND l.d <=> r.d;
SELECT count(), countIf(l.a IS NULL), sum(l.id + r.id) FROM t_ns_left l INNER JOIN t_ns_right r ON l.a <=> r.a AND if(l.d IS NULL, NULL, toIPv6(toIPv4(toUInt32(l.d))))::Nullable(IPv6) <=> if(r.d IS NULL, NULL, toIPv6(toIPv4(toUInt32(r.d))))::Nullable(IPv6);
SELECT count(), countIf(l.a IS NULL), sum(l.id + r.id) FROM t_ns_left l INNER JOIN t_ns_right r ON tuple(l.a) = tuple(r.a) AND tuple(l.d) = tuple(r.d);

DROP TABLE t_ns_left;
DROP TABLE t_ns_right;
