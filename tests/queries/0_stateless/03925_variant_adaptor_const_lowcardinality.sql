-- Regression test: ColumnConst::convertToFullColumnIfLowCardinality should
-- correctly unwrap LowCardinality inside a Const wrapper.
-- Without this, FunctionVariantAdaptor returns Const(LowCardinality(Nullable(UInt8)))
-- when Nullable(UInt8) is expected, causing a LOGICAL_ERROR.
-- https://s3.amazonaws.com/clickhouse-test-reports/json.html?PR=97242&sha=c49a5e0ed10ad8d8ac924af7287bc1c44116c271&name_0=PR&name_1=AST%20fuzzer%20%28amd_debug%29

SET allow_experimental_variant_type = 1;
SET use_variant_as_common_type = 1;
SET allow_suspicious_variant_types = 1;
SET allow_suspicious_low_cardinality_types = 1;

-- The key ingredients for reproduction:
-- 1. A merge() over tables with incompatible column types, one being LowCardinality,
--    so the column becomes Variant with a LowCardinality alternative.
-- 2. All rows in a batch come from the LowCardinality table, triggering path 1
--    in ExecutableFunctionVariantAdaptor (getGlobalDiscriminatorOfOneNoneEmptyVariantNoNulls).
-- 3. The NULL must be typed as Nullable(UInt8), not Nullable(Nothing), to avoid
--    the defaultImplementationForNothing short-circuit.

CREATE TABLE IF NOT EXISTS 03925_lc (x LowCardinality(String)) ENGINE = Memory;
CREATE TABLE IF NOT EXISTS 03925_other (x DateTime) ENGINE = Memory;
INSERT INTO 03925_lc VALUES ('a'), ('b'), ('c');

SELECT equals(CAST(NULL AS Nullable(UInt8)), x) FROM merge(currentDatabase(), '^03925_') SETTINGS optimize_move_to_prewhere = 0;

DROP TABLE 03925_lc;
DROP TABLE 03925_other;
