#include <AggregateFunctions/AggregateFunctionUniqCombined.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ARGUMENT_OUT_OF_BOUND;
}

namespace
{

AggregateFunctionPtr createAggregateFunctionUniqCombined(bool use_64_bit_hash,
    const std::string & name, const DataTypes & argument_types, const Array & params)
{
    /// log2 of the number of cells in HyperLogLog.
    /// Reasonable default value, selected to be comparable in quality with "uniq" aggregate function.
    UInt8 precision = 17;

    if (!params.empty())
    {
        if (params.size() != 1)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Aggregate function {} requires one parameter or less.",
                name);

        UInt64 precision_param = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), params[0]);
        // This range is hardcoded below
        if (precision_param > 20 || precision_param < 12)
            throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "Parameter for aggregate function {} is out of range: [12, 20].",
                name);
        precision = static_cast<UInt8>(precision_param);
    }

    if (argument_types.empty())
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Incorrect number of arguments for aggregate function {}", name);

    switch (precision) // NOLINT(bugprone-switch-missing-default-case)
    {
        case 12:
            return createAggregateFunctionWithHashType<12>(use_64_bit_hash, argument_types, params);
        case 13:
            return createAggregateFunctionWithHashType<13>(use_64_bit_hash, argument_types, params);
        case 14:
            return createAggregateFunctionWithHashType<14>(use_64_bit_hash, argument_types, params);
        case 15:
            return createAggregateFunctionWithHashType<15>(use_64_bit_hash, argument_types, params);
        case 16:
            return createAggregateFunctionWithHashType<16>(use_64_bit_hash, argument_types, params);
        case 17:
            return createAggregateFunctionWithHashType<17>(use_64_bit_hash, argument_types, params);
        case 18:
            return createAggregateFunctionWithHashType<18>(use_64_bit_hash, argument_types, params);
        case 19:
            return createAggregateFunctionWithHashType<19>(use_64_bit_hash, argument_types, params);
        case 20:
            return createAggregateFunctionWithHashType<20>(use_64_bit_hash, argument_types, params);
    }

    UNREACHABLE();
}

}

void registerAggregateFunctionUniqCombined(AggregateFunctionFactory & factory)
{
    /// uniqCombined documentation
    FunctionDocumentation::Description description_uniqCombined = R"(
Calculates the approximate number of different argument values.
It provides the result deterministically (it does not depend on the query processing order).

:::note
Since it uses a 32-bit hash for non-String types, the result will have very high error for cardinalities significantly larger than `UINT_MAX` (the error will raise quickly after a few tens of billions of distinct values).
In the case cardinalities are larger than `UINT_MAX`, you should use [`uniqCombined64`](https://clickhouse.com/docs/sql-reference/aggregate-functions/reference/uniqcombined64) instead.
:::

Compared to the uniq function, the uniqCombined function:
- Consumes several times less memory
- Calculates with several times higher accuracy
- Usually has slightly lower performance. In some scenarios, uniqCombined can perform better than uniq, for example, with distributed queries that transmit a large number of aggregation states over the network

<details>
<summary>Implementation details</summary>
This function calculates a hash (64-bit hash for String and 32-bit otherwise) for all parameters in the aggregate, then uses it in calculations.
It uses a combination of three algorithms: array, hash table, and HyperLogLog with an error correction table:
- For a small number of distinct elements, an array is used
- When the set size is larger, a hash table is used
- For a larger number of elements, HyperLogLog is used, which will occupy a fixed amount of memory
</details>
    )";
    FunctionDocumentation::Syntax syntax_uniqCombined = R"(
uniqCombined(HLL_precision)(x[, ...])
uniqCombined(x[, ...])
    )";
    FunctionDocumentation::Parameters parameters_uniqCombined = {
        {"HLL_precision", "Optional. The base-2 logarithm of the number of cells in HyperLogLog. The default value is 17, which is effectively 96 KiB of space (2^17 cells, 6 bits each). Range: [12, 20].", {"UInt8"}}
    };
    FunctionDocumentation::Arguments arguments_uniqCombined = {
        {"x", "A variable number of parameters.", {"Tuple(T)", "Array(T)", "Date", "DateTime", "String", "(U)Int*", "Float*", "Decimal"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_uniqCombined = {"Returns a UInt64-type number representing the approximate number of different argument values.", {"UInt64"}};
    FunctionDocumentation::Examples examples_uniqCombined = {
    {
        "Basic usage",
        R"(
SELECT uniqCombined(number) FROM numbers(1e6);
        )",
        R"(
┌─uniqCombined(number)─┐
│              1001148 │
└──────────────────────┘
        )"
    },
    {
        "With custom precision",
        R"(
SELECT uniqCombined(15)(number) FROM numbers(1e5);
        )",
        R"(
┌─uniqCombined(15)(number)─┐
│                   100768 │
└──────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_uniqCombined = {1, 1};
    FunctionDocumentation::Category category_uniqCombined = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation_uniqCombined = {description_uniqCombined, syntax_uniqCombined, arguments_uniqCombined, parameters_uniqCombined, returned_value_uniqCombined, examples_uniqCombined, introduced_in_uniqCombined, category_uniqCombined};

    factory.registerFunction("uniqCombined",
    {
        [](const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
        {
            return createAggregateFunctionUniqCombined(false, name, argument_types, parameters);
        },
        documentation_uniqCombined
    });
    /// uniqCombined64 documentation
    FunctionDocumentation::Description description_uniqCombined64 = R"(
Calculates the approximate number of different argument values.
It is the same as [`uniqCombined`](https://clickhouse.com/docs/sql-reference/aggregate-functions/reference/uniqcombined), but uses a 64-bit hash for all data types rather than just for the String data type.

This function provides the result deterministically (it does not depend on the query processing order).

:::note
Since it uses 64-bit hash for all types, the result does not suffer from very high error for cardinalities significantly larger than `UINT_MAX` like [`uniqCombined`](https://clickhouse.com/docs/sql-reference/aggregate-functions/reference/uniqcombined) does, which uses a 32-bit hash for non-String types.
:::

Compared to the [uniq](https://clickhouse.com/docs/sql-reference/aggregate-functions/reference/uniq) function, the uniqCombined64 function:
- Consumes several times less memory
- Calculates with several times higher accuracy

<details>
<summary>Implementation details</summary>
This function calculates a 64-bit hash for all data types for all parameters in the aggregate, then uses it in calculations.
It uses a combination of three algorithms: array, hash table, and [HyperLogLog](https://en.wikipedia.org/wiki/HyperLogLog) with an error correction table:
- For a small number of distinct elements, an array is used
- When the set size is larger, a hash table is used
- For a larger number of elements, HyperLogLog is used, which will occupy a fixed amount of memory
</details>
    )";
    FunctionDocumentation::Syntax syntax_uniqCombined64 = R"(
uniqCombined64(HLL_precision)(x[, ...])
uniqCombined64(x[, ...])
    )";
    FunctionDocumentation::Parameters parameters_uniqCombined64 = {
        {"HLL_precision", "Optional. The base-2 logarithm of the number of cells in HyperLogLog. The default value is 17, which is effectively 96 KiB of space (2^17 cells, 6 bits each). Range: [12, 20].", {"UInt8"}}
    };
    FunctionDocumentation::Arguments arguments_uniqCombined64 = {
        {"x", "A variable number of parameters.", {"Tuple(T)", "Array(T)", "Date", "DateTime", "String", "(U)Int*", "Float*", "Decimal"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_uniqCombined64 = {"Returns a UInt64-type number representing the approximate number of different argument values.", {"UInt64"}};
    FunctionDocumentation::Examples examples_uniqCombined64 = {
    {
        "Large dataset example",
        R"(
SELECT uniqCombined64(number) FROM numbers(1e10);
        )",
        R"(
┌─uniqCombined64(number)─┐
│             9998568925 │
└────────────────────────┘
        )"
    },
    {
        "Comparison with uniqCombined",
        R"(
-- uniqCombined64 with large dataset
SELECT uniqCombined64(number) FROM numbers(1e10);

-- uniqCombined with same dataset shows poor approximation
SELECT uniqCombined(number) FROM numbers(1e10);
        )",
        R"(
┌─uniqCombined64(number)─┐
│             9998568925 │ -- 10.00 billion
└────────────────────────┘
┌─uniqCombined(number)─┐
│           5545308725 │ -- 5.55 billion
└──────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_uniqCombined64 = {20, 1};
    FunctionDocumentation::Category category_uniqCombined64 = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation_uniqCombined64 = {description_uniqCombined64, syntax_uniqCombined64, arguments_uniqCombined64, parameters_uniqCombined64, returned_value_uniqCombined64, examples_uniqCombined64, introduced_in_uniqCombined64, category_uniqCombined64};

    factory.registerFunction("uniqCombined64",
        {
        [](const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
        {
            return createAggregateFunctionUniqCombined(true, name, argument_types, parameters);
        },
        documentation_uniqCombined64
    });
}

}
