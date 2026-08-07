DROP TABLE IF EXISTS compression_estimate_example;
CREATE TABLE IF NOT EXISTS compression_estimate_example (
    number UInt64
)
ENGINE = MergeTree()
ORDER BY number;

INSERT INTO compression_estimate_example
SELECT number FROM system.numbers LIMIT 100_000;

SELECT estimateCompressionRatio('DoubleDelta, T64, ZSTD')(number) AS estimate FROM compression_estimate_example FORMAT Null;
DROP TABLE IF EXISTS compression_estimate_example;