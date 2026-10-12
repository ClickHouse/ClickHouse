SELECT groupBloomFilterArrayIfState(
    materialize(CAST([toUInt64(42)] AS Array(LowCardinality(Nullable(UInt64))))),
    toUInt8(1))
FROM numbers(1)
SETTINGS allow_suspicious_low_cardinality_types = 1; -- { serverError BAD_ARGUMENTS }
