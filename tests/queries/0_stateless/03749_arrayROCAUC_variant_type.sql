-- Fixes issue: https://github.com/ClickHouse/ClickHouse/issues/95440

SET allow_experimental_variant_type = 1;
SET use_variant_as_common_type = 1;

DROP TABLE IF EXISTS test_variant_roc;

CREATE TABLE test_variant_roc (scores Array(Float64), labels Array(UInt8), roc_offsets Variant(Array(UInt8), UInt8)) ENGINE = Memory;
INSERT INTO test_variant_roc VALUES ([0.8, 0.4], [1, 0], [0, 0, 2, 2]);

SELECT arrayROCAUC(scores, labels, true, roc_offsets) AS result FROM test_variant_roc;

-- Test with explicit CAST to Variant
SELECT arrayROCAUC([0.8, 0.4], [1, 0], true, [0, 0, 2, 2]::Variant(Array(UInt8), UInt8)) AS result;

-- Simplified version
WITH partial_aucs AS (SELECT arrayROCAUC(scores, labels, true, roc_offsets) AS partial_roc_auc FROM (SELECT [0, 0, 2] AS pr_offsets, [0.8, 0.4] AS scores, [1, 0] AS labels, [0, 0, 2, 2] AS roc_offsets UNION ALL SELECT [1, 1, 2] AS pr_offsets, [0.35, 0.1] AS scores, [1, 1] AS labels, [1, 1, 2, 2] AS roc_offsets)) SELECT floor(stddevSamp(partial_roc_auc), 10) FROM partial_aucs;

DROP TABLE test_variant_roc;
