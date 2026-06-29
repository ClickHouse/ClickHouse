-- Tags: no-parallel-replicas
--
-- Regression test for STID 4054-600e: `LOGICAL_ERROR` during partition pruning
-- when a constant is cast to a `Variant` containing nested `LowCardinality`.
--
-- Root cause: `KeyCondition::applyFunctionChainToColumn` used to call
-- `convertToFullIfNeeded` on the input column which, for `ColumnConst<ColumnVariant(LowCardinality, ...)>`,
-- recursed into variant sub-columns and stripped `LowCardinality` — but `removeLowCardinality`
-- on the type only strips the outer level, leaving the type with `LowCardinality` inside the
-- `Variant` while the column had it stripped. A subsequent cast dispatched to
-- `FunctionCast::prepareUnpackDictionaries` which performed `typeid_cast<ColumnLowCardinality&>`
-- on a plain `ColumnVector`, aborting with a `LOGICAL_ERROR`.
--
-- `no-parallel-replicas`: the reproducer depends on `PARTITION BY toYYYYMM(d)` with a
-- `LowCardinality(Date)` key and a constant cast to `Variant(LowCardinality(Date), String)`
-- to reach `KeyCondition::canConstantBeWrappedByMonotonicFunctions` with the exact
-- ColumnConst shape that triggers the asymmetry. Under parallel replicas the query tree is
-- serialized with the `Date` constant folded to its raw `UInt16` representation and
-- re-parsed on the replica, where `FunctionCast::createColumnToVariantWrapper` rejects
-- `UInt16 -> Variant(LowCardinality(Date), String)` with `CANNOT_CONVERT_TYPE` before index
-- analysis runs. This is a separate, pre-existing limitation in parallel replicas constant
-- serialization, unrelated to the `KeyCondition` fix.

SET allow_experimental_variant_type = 1;
SET allow_suspicious_low_cardinality_types = 1;

DROP TABLE IF EXISTS t_04105_lc_date;

CREATE TABLE t_04105_lc_date
(
    d LowCardinality(Date),
    v UInt32
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(d)
ORDER BY d
SETTINGS allow_suspicious_low_cardinality_types = 1;

INSERT INTO t_04105_lc_date VALUES ('2026-04-01', 1), ('2026-04-19', 2), ('2026-04-20', 3);

-- This query used to abort the server with `Bad cast from type DB::ColumnVector<unsigned short> to DB::ColumnLowCardinality`.
-- Partition pruning analyzes the filter on the `LowCardinality(Date)` partition key; the constant is
-- built as a `Variant(LowCardinality(Date), String)` which triggers the outer/inner LC asymmetry.
SELECT d, v
FROM t_04105_lc_date
WHERE d >= CAST('2026-04-19' AS Variant(LowCardinality(Date), String))
ORDER BY d;

DROP TABLE t_04105_lc_date;
