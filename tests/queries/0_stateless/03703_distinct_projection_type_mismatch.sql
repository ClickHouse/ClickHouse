-- Regression test for type mismatch between DISTINCT query and aggregate projection
-- when removeTrivialWrappers strips materialize() causing LowCardinality type differences.
-- https://s3.amazonaws.com/clickhouse-test-reports/json.html?PR=98364&sha=b2717e3450567925a17d2f72dd771aa17df6fec9&name_0=PR&name_1=AST%20fuzzer%20%28amd_tsan%29

SET allow_suspicious_low_cardinality_types = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    n UInt8,
    x LowCardinality(UInt8),
    y LowCardinality(Nullable(IPv4)),
    PROJECTION p (SELECT count() GROUP BY x / 2, y % 10)
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO tab SELECT number, number % 3, number % 5 FROM numbers_mt(30);

-- The key query: materialize(10) makes the argument non-LowCardinality,
-- so the modulo result type is Nullable(UInt8) instead of LowCardinality(Nullable(UInt8)).
-- After removeTrivialWrappers strips materialize(), the query matches the projection,
-- but with mismatched types. This must not cause a crash.
SELECT DISTINCT x / 2, y % materialize(10) FROM tab ORDER BY ALL;

DROP TABLE tab;
