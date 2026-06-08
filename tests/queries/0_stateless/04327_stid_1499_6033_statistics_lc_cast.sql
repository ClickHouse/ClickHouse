SET allow_suspicious_low_cardinality_types = 1;

DROP TABLE IF EXISTS stid_1499_6033;

CREATE TABLE stid_1499_6033
(
    id Nullable(UInt32),
    b  LowCardinality(Bool) STATISTICS(minmax)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8, allow_nullable_key = 1;

INSERT INTO stid_1499_6033 SELECT number, multiIf(number < 8, true, number < 16, false, NULL) FROM numbers(24);
OPTIMIZE TABLE stid_1499_6033 FINAL;

SELECT count() FROM stid_1499_6033 WHERE b < toLowCardinality(toNullable(7));

DROP TABLE stid_1499_6033;
