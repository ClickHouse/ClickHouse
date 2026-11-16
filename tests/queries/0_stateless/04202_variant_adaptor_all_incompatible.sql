-- Regression test: FunctionVariantAdaptor must throw ILLEGAL_TYPE_OF_ARGUMENT
-- consistently when ALL Variant alternatives are incompatible with a function,
-- instead of silently returning Nullable(Nothing) which caused WHERE clauses
-- to return 0 rows with no diagnostic.
--
-- The fix: when result_types is empty after trying all alternatives,
-- throw instead of falling back to Nullable(Nothing).

SET enable_analyzer = 1;

-- Variant(UInt32, Date): neither alternative is compatible with base58Encode(String).

-- WHERE context: must throw, not silently return 0 rows.
SELECT count() FROM (
    SELECT CAST(number::UInt32 AS Variant(UInt32, Date)) AS v FROM numbers(5)
)
WHERE base58Encode(v) != ''; -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}

-- SELECT context: must throw the same error (was already consistent).
SELECT base58Encode(v) FROM (
    SELECT CAST(number::UInt32 AS Variant(UInt32, Date)) AS v FROM numbers(3)
); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}

-- Sanity check: a Variant where one alternative IS compatible works correctly.
-- base58Encode accepts String, so Variant(UInt32, String) rows with String value
-- produce a result; UInt32 rows produce NULL.
SELECT base58Encode(v) IS NOT NULL FROM (
    SELECT CAST('hello'::String AS Variant(UInt32, String)) AS v
);

-- With variant_throw_on_type_mismatch = 0, all-incompatible case must return NULL rows
-- instead of throwing, consistent with the per-row mismatch behaviour.
SET variant_throw_on_type_mismatch = 0;
SELECT base58Encode(v) IS NULL FROM (
    SELECT CAST(number::UInt32 AS Variant(UInt32, Date)) AS v FROM numbers(3)
);
SELECT count() FROM (
    SELECT CAST(number::UInt32 AS Variant(UInt32, Date)) AS v FROM numbers(3)
)
WHERE base58Encode(v) != '';
