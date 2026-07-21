-- Tags: no-old-analyzer

-- Key type sweep for the band join: types with a fixed-width encoding (integers, dates,
-- floats, decimals), types served by the generic comparator path (String, Int128), and a
-- mixed band whose lower bound is encoded while the upper one is generic. Every case is
-- verified against the cross-join oracle and byte-for-byte against `ie_join`, with the
-- match count printed so an accidentally empty fixture cannot go green vacuously.

-- Keep the written join order so the checks below exercise the orientation as written
-- instead of whatever the join order optimizer prefers.
SET query_plan_optimize_join_order_limit = 0;
SET join_algorithm = 'band_join,hash';
SET cross_to_inner_join_rewrite = 0;

DROP TABLE IF EXISTS ty_p;
DROP TABLE IF EXISTS ty_i;

CREATE TABLE ty_p (id UInt32, x Int64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE ty_i (id UInt32, a Int64, b Int64) ENGINE = MergeTree ORDER BY id;

INSERT INTO ty_p SELECT number, ((number * number + 6789) % 2147483647) % 60 FROM numbers(400);
INSERT INTO ty_i
    SELECT number, a, a + (number % 8) - 1
    FROM (SELECT number, (((number + 55) * (number + 55) + 12345) % 2147483647) % 50 AS a FROM numbers(400));

SELECT 'Int16',
    (SELECT arraySort(groupArray((p.id, i.id)))
     FROM (SELECT id, toInt16(x - 30) AS t FROM ty_p) p JOIN (SELECT id, toInt16(a - 30) AS lo, toInt16(b - 30) AS hi FROM ty_i) i ON p.t >= i.lo AND p.t <= i.hi)
        = (SELECT arraySort(groupArray((p.id, i.id)))
           FROM (SELECT id, toInt16(x - 30) AS t FROM ty_p) p, (SELECT id, toInt16(a - 30) AS lo, toInt16(b - 30) AS hi FROM ty_i) i WHERE p.t >= i.lo AND p.t <= i.hi) AS oracle_ok,
    (SELECT arraySort(groupArray((p.id, i.id)))
     FROM (SELECT id, toInt16(x - 30) AS t FROM ty_p) p JOIN (SELECT id, toInt16(a - 30) AS lo, toInt16(b - 30) AS hi FROM ty_i) i ON p.t >= i.lo AND p.t <= i.hi)
        = (SELECT arraySort(groupArray((p.id, i.id)))
           FROM (SELECT id, toInt16(x - 30) AS t FROM ty_p) p JOIN (SELECT id, toInt16(a - 30) AS lo, toInt16(b - 30) AS hi FROM ty_i) i ON p.t >= i.lo AND p.t <= i.hi
           SETTINGS join_algorithm = 'ie_join') AS ie_parity,
    (SELECT count()
     FROM (SELECT id, toInt16(x - 30) AS t FROM ty_p) p JOIN (SELECT id, toInt16(a - 30) AS lo, toInt16(b - 30) AS hi FROM ty_i) i ON p.t >= i.lo AND p.t <= i.hi) AS cnt;

SELECT 'UInt64',
    (SELECT arraySort(groupArray((p.id, i.id)))
     FROM (SELECT id, toUInt64(x) AS t FROM ty_p) p JOIN (SELECT id, toUInt64(a) AS lo, toUInt64(b) AS hi FROM ty_i) i ON p.t > i.lo AND p.t <= i.hi)
        = (SELECT arraySort(groupArray((p.id, i.id)))
           FROM (SELECT id, toUInt64(x) AS t FROM ty_p) p, (SELECT id, toUInt64(a) AS lo, toUInt64(b) AS hi FROM ty_i) i WHERE p.t > i.lo AND p.t <= i.hi) AS oracle_ok,
    (SELECT arraySort(groupArray((p.id, i.id)))
     FROM (SELECT id, toUInt64(x) AS t FROM ty_p) p JOIN (SELECT id, toUInt64(a) AS lo, toUInt64(b) AS hi FROM ty_i) i ON p.t > i.lo AND p.t <= i.hi)
        = (SELECT arraySort(groupArray((p.id, i.id)))
           FROM (SELECT id, toUInt64(x) AS t FROM ty_p) p JOIN (SELECT id, toUInt64(a) AS lo, toUInt64(b) AS hi FROM ty_i) i ON p.t > i.lo AND p.t <= i.hi
           SETTINGS join_algorithm = 'ie_join') AS ie_parity,
    (SELECT count()
     FROM (SELECT id, toUInt64(x) AS t FROM ty_p) p JOIN (SELECT id, toUInt64(a) AS lo, toUInt64(b) AS hi FROM ty_i) i ON p.t > i.lo AND p.t <= i.hi) AS cnt;

SELECT 'Date',
    (SELECT arraySort(groupArray((p.id, i.id)))
     FROM (SELECT id, toDate('2024-01-01') + x AS t FROM ty_p) p JOIN (SELECT id, toDate('2024-01-01') + a AS lo, toDate('2024-01-01') + b AS hi FROM ty_i) i ON p.t >= i.lo AND p.t < i.hi)
        = (SELECT arraySort(groupArray((p.id, i.id)))
           FROM (SELECT id, toDate('2024-01-01') + x AS t FROM ty_p) p, (SELECT id, toDate('2024-01-01') + a AS lo, toDate('2024-01-01') + b AS hi FROM ty_i) i WHERE p.t >= i.lo AND p.t < i.hi) AS oracle_ok,
    (SELECT arraySort(groupArray((p.id, i.id)))
     FROM (SELECT id, toDate('2024-01-01') + x AS t FROM ty_p) p JOIN (SELECT id, toDate('2024-01-01') + a AS lo, toDate('2024-01-01') + b AS hi FROM ty_i) i ON p.t >= i.lo AND p.t < i.hi)
        = (SELECT arraySort(groupArray((p.id, i.id)))
           FROM (SELECT id, toDate('2024-01-01') + x AS t FROM ty_p) p JOIN (SELECT id, toDate('2024-01-01') + a AS lo, toDate('2024-01-01') + b AS hi FROM ty_i) i ON p.t >= i.lo AND p.t < i.hi
           SETTINGS join_algorithm = 'ie_join') AS ie_parity,
    (SELECT count()
     FROM (SELECT id, toDate('2024-01-01') + x AS t FROM ty_p) p JOIN (SELECT id, toDate('2024-01-01') + a AS lo, toDate('2024-01-01') + b AS hi FROM ty_i) i ON p.t >= i.lo AND p.t < i.hi) AS cnt;

SELECT 'DateTime',
    (SELECT arraySort(groupArray((p.id, i.id)))
     FROM (SELECT id, toDateTime('2024-01-01 00:00:00', 'UTC') + x AS t FROM ty_p) p JOIN (SELECT id, toDateTime('2024-01-01 00:00:00', 'UTC') + a AS lo, toDateTime('2024-01-01 00:00:00', 'UTC') + b AS hi FROM ty_i) i ON p.t >= i.lo AND p.t <= i.hi)
        = (SELECT arraySort(groupArray((p.id, i.id)))
           FROM (SELECT id, toDateTime('2024-01-01 00:00:00', 'UTC') + x AS t FROM ty_p) p, (SELECT id, toDateTime('2024-01-01 00:00:00', 'UTC') + a AS lo, toDateTime('2024-01-01 00:00:00', 'UTC') + b AS hi FROM ty_i) i WHERE p.t >= i.lo AND p.t <= i.hi) AS oracle_ok,
    (SELECT count()
     FROM (SELECT id, toDateTime('2024-01-01 00:00:00', 'UTC') + x AS t FROM ty_p) p JOIN (SELECT id, toDateTime('2024-01-01 00:00:00', 'UTC') + a AS lo, toDateTime('2024-01-01 00:00:00', 'UTC') + b AS hi FROM ty_i) i ON p.t >= i.lo AND p.t <= i.hi) AS cnt;

SELECT 'Float64',
    (SELECT arraySort(groupArray((p.id, i.id)))
     FROM (SELECT id, x / 4 AS t FROM ty_p) p JOIN (SELECT id, a / 4 AS lo, b / 4 AS hi FROM ty_i) i ON p.t >= i.lo AND p.t <= i.hi)
        = (SELECT arraySort(groupArray((p.id, i.id)))
           FROM (SELECT id, x / 4 AS t FROM ty_p) p, (SELECT id, a / 4 AS lo, b / 4 AS hi FROM ty_i) i WHERE p.t >= i.lo AND p.t <= i.hi) AS oracle_ok,
    (SELECT arraySort(groupArray((p.id, i.id)))
     FROM (SELECT id, x / 4 AS t FROM ty_p) p JOIN (SELECT id, a / 4 AS lo, b / 4 AS hi FROM ty_i) i ON p.t >= i.lo AND p.t <= i.hi)
        = (SELECT arraySort(groupArray((p.id, i.id)))
           FROM (SELECT id, x / 4 AS t FROM ty_p) p JOIN (SELECT id, a / 4 AS lo, b / 4 AS hi FROM ty_i) i ON p.t >= i.lo AND p.t <= i.hi
           SETTINGS join_algorithm = 'ie_join') AS ie_parity,
    (SELECT count()
     FROM (SELECT id, x / 4 AS t FROM ty_p) p JOIN (SELECT id, a / 4 AS lo, b / 4 AS hi FROM ty_i) i ON p.t >= i.lo AND p.t <= i.hi) AS cnt;

SELECT 'Decimal64',
    (SELECT arraySort(groupArray((p.id, i.id)))
     FROM (SELECT id, toDecimal64(x, 3) / 8 AS t FROM ty_p) p JOIN (SELECT id, toDecimal64(a, 3) / 8 AS lo, toDecimal64(b, 3) / 8 AS hi FROM ty_i) i ON p.t >= i.lo AND p.t <= i.hi)
        = (SELECT arraySort(groupArray((p.id, i.id)))
           FROM (SELECT id, toDecimal64(x, 3) / 8 AS t FROM ty_p) p, (SELECT id, toDecimal64(a, 3) / 8 AS lo, toDecimal64(b, 3) / 8 AS hi FROM ty_i) i WHERE p.t >= i.lo AND p.t <= i.hi) AS oracle_ok,
    (SELECT count()
     FROM (SELECT id, toDecimal64(x, 3) / 8 AS t FROM ty_p) p JOIN (SELECT id, toDecimal64(a, 3) / 8 AS lo, toDecimal64(b, 3) / 8 AS hi FROM ty_i) i ON p.t >= i.lo AND p.t <= i.hi) AS cnt;

-- Strings take the generic comparator path but must still run inside the band join
SELECT 'String plan', count() > 0 FROM (EXPLAIN
    SELECT count() FROM (SELECT id, leftPad(toString(x), 4, '0') AS t FROM ty_p) p
    JOIN (SELECT id, leftPad(toString(a), 4, '0') AS lo, leftPad(toString(b), 4, '0') AS hi FROM ty_i) i ON p.t >= i.lo AND p.t <= i.hi)
WHERE explain LIKE '%BandJoin%';

SELECT 'String',
    (SELECT arraySort(groupArray((p.id, i.id)))
     FROM (SELECT id, leftPad(toString(x), 4, '0') AS t FROM ty_p) p JOIN (SELECT id, leftPad(toString(a), 4, '0') AS lo, leftPad(toString(b), 4, '0') AS hi FROM ty_i) i ON p.t >= i.lo AND p.t <= i.hi)
        = (SELECT arraySort(groupArray((p.id, i.id)))
           FROM (SELECT id, leftPad(toString(x), 4, '0') AS t FROM ty_p) p, (SELECT id, leftPad(toString(a), 4, '0') AS lo, leftPad(toString(b), 4, '0') AS hi FROM ty_i) i WHERE p.t >= i.lo AND p.t <= i.hi) AS oracle_ok,
    (SELECT arraySort(groupArray((p.id, i.id)))
     FROM (SELECT id, leftPad(toString(x), 4, '0') AS t FROM ty_p) p JOIN (SELECT id, leftPad(toString(a), 4, '0') AS lo, leftPad(toString(b), 4, '0') AS hi FROM ty_i) i ON p.t >= i.lo AND p.t <= i.hi)
        = (SELECT arraySort(groupArray((p.id, i.id)))
           FROM (SELECT id, leftPad(toString(x), 4, '0') AS t FROM ty_p) p JOIN (SELECT id, leftPad(toString(a), 4, '0') AS lo, leftPad(toString(b), 4, '0') AS hi FROM ty_i) i ON p.t >= i.lo AND p.t <= i.hi
           SETTINGS join_algorithm = 'ie_join') AS ie_parity,
    (SELECT count()
     FROM (SELECT id, leftPad(toString(x), 4, '0') AS t FROM ty_p) p JOIN (SELECT id, leftPad(toString(a), 4, '0') AS lo, leftPad(toString(b), 4, '0') AS hi FROM ty_i) i ON p.t >= i.lo AND p.t <= i.hi) AS cnt;

SELECT 'LowCardinality(String)',
    (SELECT arraySort(groupArray((p.id, i.id)))
     FROM (SELECT id, toLowCardinality(leftPad(toString(x), 4, '0')) AS t FROM ty_p) p JOIN (SELECT id, toLowCardinality(leftPad(toString(a), 4, '0')) AS lo, toLowCardinality(leftPad(toString(b), 4, '0')) AS hi FROM ty_i) i ON p.t >= i.lo AND p.t <= i.hi)
        = (SELECT arraySort(groupArray((p.id, i.id)))
           FROM (SELECT id, leftPad(toString(x), 4, '0') AS t FROM ty_p) p, (SELECT id, leftPad(toString(a), 4, '0') AS lo, leftPad(toString(b), 4, '0') AS hi FROM ty_i) i WHERE p.t >= i.lo AND p.t <= i.hi) AS oracle_ok,
    (SELECT count()
     FROM (SELECT id, toLowCardinality(leftPad(toString(x), 4, '0')) AS t FROM ty_p) p JOIN (SELECT id, toLowCardinality(leftPad(toString(a), 4, '0')) AS lo, toLowCardinality(leftPad(toString(b), 4, '0')) AS hi FROM ty_i) i ON p.t >= i.lo AND p.t <= i.hi) AS cnt;

SELECT 'Int128',
    (SELECT arraySort(groupArray((p.id, i.id)))
     FROM (SELECT id, toInt128(x - 30) AS t FROM ty_p) p JOIN (SELECT id, toInt128(a - 30) AS lo, toInt128(b - 30) AS hi FROM ty_i) i ON p.t > i.lo AND p.t < i.hi)
        = (SELECT arraySort(groupArray((p.id, i.id)))
           FROM (SELECT id, toInt128(x - 30) AS t FROM ty_p) p, (SELECT id, toInt128(a - 30) AS lo, toInt128(b - 30) AS hi FROM ty_i) i WHERE p.t > i.lo AND p.t < i.hi) AS oracle_ok,
    (SELECT count()
     FROM (SELECT id, toInt128(x - 30) AS t FROM ty_p) p JOIN (SELECT id, toInt128(a - 30) AS lo, toInt128(b - 30) AS hi FROM ty_i) i ON p.t > i.lo AND p.t < i.hi) AS cnt;

-- Mixed encodability: the lower bound compares in Int64 (encoded), the upper one is cast to
-- the Int128 common type (generic path); the two bounds decide independently
SELECT 'mixed Int64/Int128',
    (SELECT arraySort(groupArray((p.id, i.id)))
     FROM (SELECT id, x AS t FROM ty_p) p JOIN (SELECT id, a AS lo, toInt128(b) AS hi FROM ty_i) i ON p.t >= i.lo AND p.t <= i.hi)
        = (SELECT arraySort(groupArray((p.id, i.id)))
           FROM (SELECT id, x AS t FROM ty_p) p, (SELECT id, a AS lo, toInt128(b) AS hi FROM ty_i) i WHERE p.t >= i.lo AND p.t <= i.hi) AS oracle_ok,
    (SELECT arraySort(groupArray((p.id, i.id)))
     FROM (SELECT id, x AS t FROM ty_p) p JOIN (SELECT id, a AS lo, toInt128(b) AS hi FROM ty_i) i ON p.t >= i.lo AND p.t <= i.hi)
        = (SELECT arraySort(groupArray((p.id, i.id)))
           FROM (SELECT id, x AS t FROM ty_p) p JOIN (SELECT id, a AS lo, toInt128(b) AS hi FROM ty_i) i ON p.t >= i.lo AND p.t <= i.hi
           SETTINGS join_algorithm = 'ie_join') AS ie_parity,
    (SELECT count()
     FROM (SELECT id, x AS t FROM ty_p) p JOIN (SELECT id, a AS lo, toInt128(b) AS hi FROM ty_i) i ON p.t >= i.lo AND p.t <= i.hi) AS cnt;

-- Different integer widths on the two sides of one bound are cast to the common type
SELECT 'Int32 vs Int64',
    (SELECT arraySort(groupArray((p.id, i.id)))
     FROM (SELECT id, toInt32(x) AS t FROM ty_p) p JOIN (SELECT id, a AS lo, b AS hi FROM ty_i) i ON p.t >= i.lo AND p.t <= i.hi)
        = (SELECT arraySort(groupArray((p.id, i.id)))
           FROM (SELECT id, toInt32(x) AS t FROM ty_p) p, (SELECT id, a AS lo, b AS hi FROM ty_i) i WHERE p.t >= i.lo AND p.t <= i.hi) AS oracle_ok,
    (SELECT count()
     FROM (SELECT id, toInt32(x) AS t FROM ty_p) p JOIN (SELECT id, a AS lo, b AS hi FROM ty_i) i ON p.t >= i.lo AND p.t <= i.hi) AS cnt;

DROP TABLE ty_p;
DROP TABLE ty_i;
