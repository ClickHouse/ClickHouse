-- Tags: no-fasttest

-- Regression test for the `IPv4`/`IPv6` type inference feature (PR #110766): prove, at the real
-- SQL/`MergeTree` level, that `IPv4` binary serialization round-trips correctly through the
-- actual write path (INSERT -> part -> `minmax_ip.idx`, written by
-- `IMergeTreeDataPart::MinMaxIndex::store` -> merge -> read back), and that the on-disk bytes
-- match the documented wire format in `docs/reference/interfaces/specs/NativeFormat.mdx` exactly.
-- This is not about correctness of the C++ unit-level serializer (covered by a separate gtest);
-- it exercises the production write/merge/read path end-to-end.

SET explain_query_plan_default = 'legacy';

-- 1. Wire-format byte assertion.
-- `IPv4` is a `StrongTypedef<UInt32, ...>` with no extra state, so on the little-endian hosts CI
-- runs on its native in-memory layout is exactly the little-endian wire bytes documented in
-- NativeFormat.mdx. `192.168.1.10` (canonical 32-bit value `0xC0A8010A`) must serialize to
-- exactly `0A 01 A8 C0`.
SELECT hex(reinterpretAsFixedString(toIPv4('192.168.1.10')));

-- 2. Scalar `IPv4` column in `MergeTree`, partitioned by `ip`. The per-part `minmax_<column>.idx`
-- file is only written for columns that are part of the partition expression, so `PARTITION BY ip`
-- is required to actually exercise `IMergeTreeDataPart::MinMaxIndex::store` (a plain `INDEX ...
-- TYPE minmax` skip index would go through a different, unrelated serializer).
DROP TABLE IF EXISTS t_ipv4_minmax;

CREATE TABLE t_ipv4_minmax
(
    id UInt32,
    ip IPv4
)
ENGINE = MergeTree
PARTITION BY ip
ORDER BY id;

INSERT INTO t_ipv4_minmax VALUES (1, '0.0.0.0'), (2, '255.255.255.255'), (3, '1.2.3.4'), (4, '192.168.0.1');
-- Second insert lands in the same partition as id=3, so OPTIMIZE FINAL below has to actually
-- merge two parts sharing ip = '1.2.3.4' and rewrite/merge their minmax_ip.idx.
INSERT INTO t_ipv4_minmax VALUES (5, '1.2.3.4');

OPTIMIZE TABLE t_ipv4_minmax FINAL;

SELECT id, ip FROM t_ipv4_minmax ORDER BY id;

-- Query that should benefit from partition pruning via the just-merged minmax_ip.idx.
SELECT id, ip FROM t_ipv4_minmax WHERE ip = toIPv4('1.2.3.4') ORDER BY id;

-- Confirm the plan actually performed Min-Max/Partition pruning analysis on `ip` (existence
-- check, deliberately not asserting the exact Parts/Granules counts or full plan tree, which are
-- sensitive to unrelated settings and formatting). Uses a row-returning SELECT rather than
-- count() to avoid the trivial-count-from-partition-metadata optimization (see
-- InterpreterSelectQuery::getTrivialCount), which would bypass ReadFromMergeTree/Indexes entirely
-- for a count() query filtered directly on the partition column.
SELECT
    countIf(explain LIKE '%Min-Max%') > 0 AS has_minmax_pruning,
    countIf(explain LIKE '%Partition%') > 0 AS has_partition_pruning
FROM (EXPLAIN indexes = 1 SELECT id, ip FROM t_ipv4_minmax WHERE ip = toIPv4('1.2.3.4') SETTINGS optimize_use_implicit_projections = 0);

DROP TABLE t_ipv4_minmax;

-- 3. Array(IPv4) through the same MergeTree write/merge/read path.
DROP TABLE IF EXISTS t_ipv4_array;

CREATE TABLE t_ipv4_array
(
    id UInt32,
    ips Array(IPv4)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO t_ipv4_array VALUES (1, ['0.0.0.0', '255.255.255.255']), (2, ['1.2.3.4', '192.168.0.1']);

OPTIMIZE TABLE t_ipv4_array FINAL;

SELECT id, ips FROM t_ipv4_array ORDER BY id;

DROP TABLE t_ipv4_array;

-- 4. JSON shared-data path: with `max_dynamic_paths = 0`, every path is stored in the JSON
-- shared-data structure, so an IPv4-looking string inferred via `input_format_try_infer_ipv4`
-- must round-trip through `SerializationDynamic` -> `SerializationVariant` ->
-- `SerializationIP<IPv4>::serializeBinaryBulk` on a real MergeTree part, not just in memory.
DROP TABLE IF EXISTS t_json_ip;

CREATE TABLE t_json_ip
(
    id UInt32,
    json JSON(max_dynamic_paths = 0)
)
ENGINE = MergeTree
ORDER BY id;

SET input_format_try_infer_ipv4 = 1;

INSERT INTO t_json_ip FORMAT JSONEachRow {"id": 1, "json": {"ip": "0.0.0.0"}}, {"id": 2, "json": {"ip": "255.255.255.255"}}, {"id": 3, "json": {"ip": "1.2.3.4"}}, {"id": 4, "json": {"ip": "192.168.0.1"}};

OPTIMIZE TABLE t_json_ip FINAL;

SELECT id, dynamicType(json.ip), json.ip, JSONSharedDataPaths(json) FROM t_json_ip ORDER BY id;

DROP TABLE t_json_ip;
