#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsBloomFilter.h>


namespace DB
{

REGISTER_FUNCTION(BloomFilter)
{
    /// Documentation for bloomFilterContains
    FunctionDocumentation::Description description_bloomFilterContains = R"(
Checks whether a value is probably present in a Bloom filter built by [`groupBloomFilter`](/sql-reference/aggregate-functions/reference/groupbloomfilter).

Returns `1` if the value is probably in the filter (may have false positives),
or `0` if the value is definitely not in the filter (no false negatives).

The false positive rate is controlled by the `false_positive_rate` parameter of `groupBloomFilter`.
    )";
    FunctionDocumentation::Syntax syntax_bloomFilterContains = "bloomFilterContains(bloom_filter, value)";
    FunctionDocumentation::Arguments arguments_bloomFilterContains = {
        {"bloom_filter", "Bloom filter state. [`AggregateFunction(groupBloomFilter, T)`](/sql-reference/data-types/aggregatefunction)."},
        {"value", "Value to check for. Must be the same type as was used to build the filter."}
    };
    FunctionDocumentation::ReturnedValue returned_value_bloomFilterContains = {
        "Returns `1` if the value is probably present in the filter, `0` if it is definitely absent.",
        {"UInt8"}
    };
    FunctionDocumentation::Examples examples_bloomFilterContains = {
        {
            "Basic usage",
            R"(
SELECT bloomFilterContains(groupBloomFilterState(1000)(number), toUInt64(42)) AS result
FROM numbers(100)
            )",
            R"(
┌─result─┐
│      1 │
└────────┘
            )"
        },
        {
            "Check a value absent from the filter",
            R"(
SELECT bloomFilterContains(groupBloomFilterState(1000)(number), toUInt64(200)) AS result
FROM numbers(100)
            )",
            R"(
┌─result─┐
│      0 │
└────────┘
            )"
        },
        {
            "Find new values using WITH clause",
            R"(
WITH (
    SELECT groupBloomFilterState(1000)(number)
    FROM numbers(100)
) AS old_bloom
SELECT count() AS new_values_count
FROM numbers(200)
WHERE number >= 100
    AND NOT bloomFilterContains(old_bloom, number)
            )",
            R"(
┌─new_values_count─┐
│              100 │
└──────────────────┘
            )"
        }
    };
    FunctionDocumentation::IntroducedIn introduced_in_bloomFilterContains = {25, 5};
    FunctionDocumentation::Category category_bloomFilterContains = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation_bloomFilterContains = {
        description_bloomFilterContains,
        syntax_bloomFilterContains,
        arguments_bloomFilterContains,
        {},
        returned_value_bloomFilterContains,
        examples_bloomFilterContains,
        introduced_in_bloomFilterContains,
        category_bloomFilterContains
    };

    factory.registerFunction<FunctionBloomFilterContains>(documentation_bloomFilterContains);
}

}
