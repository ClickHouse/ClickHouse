-- An explicit state version 0 of an `AggregateFunction` inside a `SimpleAggregateFunction` survives the version pin of
-- `CREATE TABLE`, `ALTER TABLE ... ADD COLUMN` and `CREATE TABLE ... AS SELECT`: parts written before a reload are read back.

SET flatten_nested = 0;
SET allow_suspicious_variant_types = 1;

DROP TABLE IF EXISTS saf_v0;
DROP TABLE IF EXISTS saf_v0_ctas;
DROP TABLE IF EXISTS saf_v0_variant;

CREATE TABLE saf_v0
(
    k UInt8,
    p0 AggregateFunction(0, quantileDeterministic, UInt64, UInt64),
    s0 SimpleAggregateFunction(anyLast, AggregateFunction(0, quantileDeterministic, UInt64, UInt64)),
    sv SimpleAggregateFunction(anyLast, AggregateFunction(quantileDeterministic, UInt64, UInt64)),
    t0 SimpleAggregateFunction(anyLast, Tuple(AggregateFunction(0, quantileDeterministic, UInt64, UInt64), AggregateFunction(quantileDeterministic, UInt64, UInt64))),
    a0 Array(SimpleAggregateFunction(anyLast, AggregateFunction(0, quantileDeterministic, UInt64, UInt64))),
    n0 Nested(s SimpleAggregateFunction(anyLast, AggregateFunction(0, quantileDeterministic, UInt64, UInt64))),
    q0 AggregateFunction(0, uniq, UInt64),
    u0 SimpleAggregateFunction(any, AggregateFunction(0, uniq, UInt64)),
    m0 SimpleAggregateFunction(anyLast, AggregateFunction(0, sumMap, Array(UInt8), Array(Decimal32(2)))),
    ns0 SimpleAggregateFunction(anyLast, SimpleAggregateFunction(anyLast, AggregateFunction(0, quantileDeterministic, UInt64, UInt64)))
)
ENGINE = MergeTree ORDER BY k;

ALTER TABLE saf_v0 ADD COLUMN c0 SimpleAggregateFunction(anyLast, AggregateFunction(0, quantileDeterministic, UInt64, UInt64));

INSERT INTO saf_v0 SELECT
    number % 3,
    quantileDeterministicState(number, number),
    quantileDeterministicState(number, number),
    quantileDeterministicState(number, number),
    (quantileDeterministicState(number, number), quantileDeterministicState(number, number)),
    [quantileDeterministicState(number, number)],
    [tuple(quantileDeterministicState(number, number))],
    uniqState(number),
    uniqState(number),
    sumMapState([toUInt8(number % 2)], [toDecimal32(number, 2)]),
    quantileDeterministicState(number, number),
    quantileDeterministicState(number, number)
FROM numbers(30) GROUP BY number % 3;

CREATE TABLE saf_v0_ctas ENGINE = MergeTree ORDER BY k AS
SELECT number % 3 AS k, CAST(quantileDeterministicState(number, number), 'SimpleAggregateFunction(anyLast, AggregateFunction(0, quantileDeterministic, UInt64, UInt64))') AS s0
FROM numbers(30) GROUP BY k;

CREATE TABLE saf_v0_variant
(
    k UInt8,
    v SimpleAggregateFunction(anyLast, Variant(AggregateFunction(0, quantileDeterministic, UInt64, UInt64), AggregateFunction(1, quantileDeterministic, UInt64, UInt64)))
)
ENGINE = MergeTree ORDER BY k;
INSERT INTO saf_v0_variant SELECT 0, CAST(quantileDeterministicState(number, number), 'AggregateFunction(0, quantileDeterministic, UInt64, UInt64)') FROM numbers(30);
INSERT INTO saf_v0_variant SELECT 1, CAST(quantileDeterministicState(number, number), 'AggregateFunction(1, quantileDeterministic, UInt64, UInt64)') FROM numbers(30);

SELECT table, name, type FROM system.columns
WHERE database = currentDatabase() AND table IN ('saf_v0', 'saf_v0_ctas', 'saf_v0_variant') AND name != 'k' ORDER BY table, position;
-- An explicit 0 keeps the layout of the plain `AggregateFunction(0, ...)` column; the unversioned spelling gets the current version.
SELECT k, hex(s0) = hex(p0), hex(sv) != hex(p0), hex(t0.1) = hex(p0), hex(t0.2) = hex(sv), hex(a0[1]) = hex(p0),
    hex((n0[1]).s) = hex(p0), hex(c0) = hex(p0), hex(u0) = hex(q0), hex(ns0) = hex(p0)
FROM saf_v0 ORDER BY k SETTINGS enable_parallel_replicas = 0;

DETACH TABLE saf_v0;
ATTACH TABLE saf_v0;
DETACH TABLE saf_v0_ctas;
ATTACH TABLE saf_v0_ctas;
DETACH TABLE saf_v0_variant;
ATTACH TABLE saf_v0_variant;

SELECT table, name, type FROM system.columns
WHERE database = currentDatabase() AND table IN ('saf_v0', 'saf_v0_ctas', 'saf_v0_variant') AND name != 'k' ORDER BY table, position;
SELECT k, finalizeAggregation(s0), finalizeAggregation(sv), finalizeAggregation(t0.1), finalizeAggregation(t0.2), finalizeAggregation(a0[1]),
    finalizeAggregation((n0[1]).s), finalizeAggregation(c0), finalizeAggregation(u0), finalizeAggregation(m0), finalizeAggregation(ns0)
FROM saf_v0 ORDER BY k;
SELECT k, finalizeAggregation(s0) FROM saf_v0_ctas ORDER BY k;
SELECT k, variantType(v), finalizeAggregation(variantElement(v, 'AggregateFunction(quantileDeterministic, UInt64, UInt64)')) FROM saf_v0_variant WHERE k = 0;
SELECT k, variantType(v), finalizeAggregation(variantElement(v, 'AggregateFunction(1, quantileDeterministic, UInt64, UInt64)')) FROM saf_v0_variant WHERE k = 1;
SELECT k, hex(s0) = hex(p0), hex(sv) != hex(p0), hex(t0.1) = hex(p0), hex(t0.2) = hex(sv), hex(a0[1]) = hex(p0),
    hex((n0[1]).s) = hex(p0), hex(c0) = hex(p0), hex(u0) = hex(q0), hex(ns0) = hex(p0)
FROM saf_v0 ORDER BY k SETTINGS enable_parallel_replicas = 0;

DROP TABLE saf_v0;
DROP TABLE saf_v0_ctas;
DROP TABLE saf_v0_variant;
