-- Test: ABR rejects nullable time columns.
-- Verifies that Nullable(T) and LowCardinality(Nullable(T)) temporal columns
-- cannot be used for ABR routing.

SET allow_suspicious_low_cardinality_types = 1;

DROP TABLE IF EXISTS abr_nullable_time;
DROP TABLE IF EXISTS abr_low_cardinality_nullable_time;
DROP TABLE IF EXISTS nullable_time_1;
DROP TABLE IF EXISTS nullable_time_10;
DROP TABLE IF EXISTS low_cardinality_nullable_time_1;

CREATE TABLE nullable_time_1
(
    _sample_interval UInt64 DEFAULT 1,
    ts Nullable(DateTime),
    value UInt64
)
ENGINE = MergeTree
PARTITION BY ts
ORDER BY ts
SETTINGS allow_nullable_key = 1;

CREATE TABLE nullable_time_10
(
    _sample_interval UInt64 DEFAULT 10,
    ts Nullable(DateTime),
    value UInt64
)
ENGINE = MergeTree
PARTITION BY ts
ORDER BY ts
SETTINGS allow_nullable_key = 1;

CREATE TABLE abr_nullable_time
ENGINE = ABR('nullable_time', [1, 10], ('ts')); -- {serverError BAD_ARGUMENTS}

CREATE TABLE low_cardinality_nullable_time_1
(
    _sample_interval UInt64 DEFAULT 1,
    ts LowCardinality(Nullable(DateTime)),
    value UInt64
)
ENGINE = MergeTree
PARTITION BY ts
ORDER BY ts
SETTINGS allow_nullable_key = 1;

CREATE TABLE abr_low_cardinality_nullable_time
ENGINE = ABR('low_cardinality_nullable_time', [1], ('ts')); -- {serverError BAD_ARGUMENTS}

DROP TABLE IF EXISTS nullable_time_1;
DROP TABLE IF EXISTS nullable_time_10;
DROP TABLE IF EXISTS low_cardinality_nullable_time_1;
