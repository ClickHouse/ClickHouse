SELECT
    *,
    * APPLY (toTypeName)
FROM
(
    SELECT
        bl,
        anyIf(n, cond) IGNORE NULLS AS any_ignore,
        anyIf(n, cond) RESPECT NULLS AS any_respect,
        anyLastIf(n, cond) IGNORE NULLS AS last_ignore,
        anyLastIf(n, cond) RESPECT NULLS AS last_respect,
        anyIf(nullable_n, cond) IGNORE NULLS AS any_nullable_ignore,
        anyIf(nullable_n, cond) RESPECT NULLS AS any_nullable_respect,
        anyLastIf(nullable_n, cond) IGNORE NULLS AS last_nullable_ignore,
        anyLastIf(nullable_n, cond) RESPECT NULLS AS last_nullable_respect
    FROM
        (
            SELECT
                number AS n,
                rand() > pow(2, 31) as cond,
                if(cond, NULL, n) as nullable_n,
                blockNumber() AS bl
            FROM numbers(10000)
        )
    GROUP BY bl
)
WHERE
      any_ignore != any_respect
   OR toTypeName(any_ignore) != toTypeName(any_respect)
   OR last_ignore != last_respect
   OR toTypeName(last_ignore) != toTypeName(last_respect)
   OR any_nullable_ignore != any_nullable_respect
   OR toTypeName(any_nullable_ignore) != toTypeName(any_nullable_respect)
   OR last_nullable_ignore != last_nullable_respect
   OR toTypeName(last_nullable_ignore) != toTypeName(last_nullable_respect);

-- { echoOn }
Select anyOrNull(tp) FROM (Select (number, number) as tp from numbers(10)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
Select anyOrNull(tp) FROM (Select (number, number) as tp from numbers(10)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT
    any(tp) AS default,
    toTypeName(default) as default_type,
    any(tp) RESPECT NULLS AS respect,
    toTypeName(respect) as respect_type
FROM
(
    SELECT (toLowCardinality(number), number) AS tp
    FROM numbers(10)
);

SELECT first_value_respect_nullsMerge(t) FROM (Select first_value_respect_nullsState(number) as t FROM numbers(0));
SELECT first_value_respect_nullsMerge(t) FROM (Select first_value_respect_nullsState(number::Nullable(UInt8)) as t FROM numbers(0));
SELECT first_value_respect_nullsMerge(t) FROM (Select first_value_respect_nullsState(number::LowCardinality(Nullable(UInt8))) as t FROM numbers(0)) settings allow_suspicious_low_cardinality_types=1;

SELECT first_value_respect_nullsOrNullMerge(t) FROM (Select first_value_respect_nullsOrNullState(number) as t FROM numbers(0));
SELECT first_value_respect_nullsMerge(t) FROM (Select first_value_respect_nullsOrNullState(number) as t FROM numbers(0)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT first_value_respect_nullsOrNullMerge(t) FROM (Select first_value_respect_nullsState(number) as t FROM numbers(0)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }


SELECT first_value_respect_nullsMerge(t) FROM (Select first_value_respect_nullsState(dummy) as t FROM system.one);
SELECT first_value_respect_nullsMerge(t) FROM (Select first_value_respect_nullsState(dummy::Nullable(UInt8)) as t FROM system.one);
SELECT first_value_respect_nullsMerge(t) FROM (Select first_value_respect_nullsState(NULL) as t FROM system.one);
SELECT first_value_respect_nullsMerge(t) FROM (Select first_value_respect_nullsState(NULL::Nullable(UInt8)) as t FROM system.one);

-- Assert sanitizer: passing NULL (not Nullable() with different values is accepted and ignored)
SELECT
    anyLastIf(n, cond) RESPECT NULLS,
    anyLastIf(nullable_n, cond) RESPECT NULLS
FROM
(
    SELECT
        number AS n,
        NULL as cond,
        number::Nullable(Int64) as nullable_n
    FROM numbers(10000)
);
