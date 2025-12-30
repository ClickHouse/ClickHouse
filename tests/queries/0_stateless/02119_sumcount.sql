-- Integer types are added as integers
SELECT toTypeName(sumCount(v)), sumCount(v) FROM
(
    SELECT v FROM
    (
        SELECT '9007199254740992'::UInt64 AS v
        UNION ALL
        SELECT '1'::UInt64 AS v
        UNION ALL SELECT '1'::UInt64 AS v
    )
    ORDER BY v
);
SELECT toTypeName(sumCount(v)), sumCount(v) FROM
(
    SELECT v FROM
    (
        SELECT '9007199254740992'::Nullable(UInt64) AS v
        UNION ALL
        SELECT '1'::Nullable(UInt64) AS v
        UNION ALL
        SELECT '1'::Nullable(UInt64) AS v
    )
    ORDER BY v
);

SET allow_suspicious_low_cardinality_types=1;

SELECT toTypeName(sumCount(v)), sumCount(v) FROM
(
    SELECT v FROM
	(
        SELECT '9007199254740992'::LowCardinality(UInt64) AS v
        UNION ALL
        SELECT '1'::LowCardinality(UInt64) AS v
        UNION ALL
        SELECT '1'::LowCardinality(UInt64) AS v
    )
    ORDER BY v
);
SELECT toTypeName(sumCount(v)), sumCount(v) FROM
(
    SELECT v FROM
    (
        SELECT '9007199254740992'::LowCardinality(Nullable(UInt64)) AS v
        UNION ALL
        SELECT '1'::LowCardinality(Nullable(UInt64)) AS v
        UNION ALL
        SELECT '1'::LowCardinality(Nullable(UInt64)) AS v
    )
    ORDER BY v
);

-- -- Float64 types are added as Float64
SELECT toTypeName(sumCount(v)), sumCount(v) FROM
(
    SELECT v FROM
    (
        SELECT '9007199254740992'::Float64 AS v
        UNION ALL
        SELECT '1'::Float64 AS v
        UNION ALL SELECT '1'::Float64 AS v
    )
    ORDER BY v
);
SELECT toTypeName(sumCount(v)), sumCount(v) FROM
(
    SELECT v FROM
    (
        SELECT '9007199254740992'::Nullable(Float64) AS v
        UNION ALL
        SELECT '1'::Nullable(Float64) AS v
        UNION ALL
        SELECT '1'::Nullable(Float64) AS v
    )
    ORDER BY v
);
SELECT toTypeName(sumCount(v)), sumCount(v) FROM
(
    SELECT v FROM
    (
        SELECT '9007199254740992'::LowCardinality(Float64) AS v
        UNION ALL
        SELECT '1'::LowCardinality(Float64) AS v
        UNION ALL
        SELECT '1'::LowCardinality(Float64) AS v
    )
    ORDER BY v
);
SELECT toTypeName(sumCount(v)), sumCount(v) FROM
(
    SELECT v FROM
    (
        SELECT '9007199254740992'::LowCardinality(Nullable(Float64)) AS v
        UNION ALL
        SELECT '1'::LowCardinality(Nullable(Float64)) AS v
        UNION ALL
        SELECT '1'::LowCardinality(Nullable(Float64)) AS v
    )
    ORDER BY v
);

-- -- Float32 are added as Float64
SELECT toTypeName(sumCount(v)), sumCount(v) FROM
(
    SELECT v FROM
    (
        SELECT '16777216'::Float32 AS v
        UNION ALL
        SELECT '1'::Float32 AS v
        UNION ALL
        SELECT '1'::Float32 AS v
    )
    ORDER BY v
);
SELECT toTypeName(sumCount(v)), sumCount(v) FROM
(
    SELECT v FROM
    (
        SELECT '16777216'::Nullable(Float32) AS v
        UNION ALL
        SELECT '1'::Nullable(Float32) AS v
        UNION ALL
        SELECT '1'::Nullable(Float32) AS v
    )
    ORDER BY v
);
SELECT toTypeName(sumCount(v)), sumCount(v) FROM
(
    SELECT v FROM
    (
        SELECT '16777216'::LowCardinality(Float32) AS v
        UNION ALL
        SELECT '1'::LowCardinality(Float32) AS v
        UNION ALL
        SELECT '1'::LowCardinality(Float32) AS v
    )
    ORDER BY v
);
SELECT toTypeName(sumCount(v)), sumCount(v) FROM
(
    SELECT v FROM
    (
        SELECT '16777216'::LowCardinality(Nullable(Float32)) AS v
        UNION ALL
        SELECT '1'::LowCardinality(Nullable(Float32)) AS v
        UNION ALL
        SELECT '1'::LowCardinality(Nullable(Float32)) AS v
    )
    ORDER BY v
);

-- Small integer types use their sign/unsigned 64 byte supertype
SELECT toTypeName(sumCount(number::Int8)), sumCount(number::Int8) FROM numbers(120);
SELECT toTypeName(sumCount(number::UInt8)), sumCount(number::UInt8) FROM numbers(250);

-- Greater integers use their own type
SELECT toTypeName(sumCount(v)), sumCount(v) FROM (SELECT '1'::Int128 AS v FROM numbers(100));
SELECT toTypeName(sumCount(v)), sumCount(v) FROM (SELECT '1'::Int256 AS v FROM numbers(100));
SELECT toTypeName(sumCount(v)), sumCount(v) FROM (SELECT '1'::UInt128 AS v FROM numbers(100));
SELECT toTypeName(sumCount(v)), sumCount(v) FROM (SELECT '1'::UInt256 AS v FROM numbers(100));

-- Decimal types
SELECT toTypeName(sumCount(v)), sumCount(v) FROM (SELECT '1.001'::Decimal(3, 3) AS v FROM numbers(100));

-- Other types
SELECT toTypeName(sumCount(v)), sumCount(v) FROM (SELECT 'a'::String AS v); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toTypeName(sumCount(v)), sumCount(v) FROM (SELECT now()::DateTime AS v); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }


-- SumCountIf
SELECT sumCountIf(n, n > 10) FROM (SELECT number AS n FROM system.numbers LIMIT 100 );
SELECT sumCountIf(n, n > 10) FROM (SELECT toNullable(number) AS n FROM system.numbers LIMIT 100);
SELECT sumCountIf(n, n > 10) FROM (SELECT If(number % 2 == 0, number, NULL) AS n FROM system.numbers LIMIT 100);
