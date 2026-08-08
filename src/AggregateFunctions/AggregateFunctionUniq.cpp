#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionUniq.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>

#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypeIPv4andIPv6.h>

#include <Core/Settings.h>

namespace DB
{

namespace Setting
{
    extern const SettingsMaxThreads max_threads;
}

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


namespace
{


/** `DataForVariadic` is a data structure that will be used for `uniq` aggregate function of multiple arguments.
  * It differs, for example, in that it uses a trivial hash function, since `uniq` of many arguments first hashes them out itself.
  */
template <typename Data, template <bool, bool> typename DataForVariadic>
AggregateFunctionPtr
createAggregateFunctionUniq(const std::string & name, const DataTypes & argument_types, const Array & params, const Settings *)
{
    assertNoParameters(name, params);

    if (argument_types.empty())
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Incorrect number of arguments for aggregate function {}", name);

    bool use_exact_hash_function = !isAllArgumentsContiguousInMemory(argument_types);

    if (argument_types.size() == 1)
    {
        const IDataType & argument_type = *argument_types[0];

        AggregateFunctionPtr res(createWithNumericType<AggregateFunctionUniq, Data>(*argument_types[0], argument_types));

        WhichDataType which(argument_type);
        if (res)
            return res;
        if (which.isDate())
            return std::make_shared<AggregateFunctionUniq<DataTypeDate::FieldType, Data>>(argument_types);
        if (which.isDate32())
            return std::make_shared<AggregateFunctionUniq<DataTypeDate32::FieldType, Data>>(argument_types);
        if (which.isDateTime())
            return std::make_shared<AggregateFunctionUniq<DataTypeDateTime::FieldType, Data>>(argument_types);
        if (which.isStringOrFixedString())
            return std::make_shared<AggregateFunctionUniq<String, Data>>(argument_types);
        if (which.isUUID())
            return std::make_shared<AggregateFunctionUniq<DataTypeUUID::FieldType, Data>>(argument_types);
        if (which.isIPv4())
            return std::make_shared<AggregateFunctionUniq<DataTypeIPv4::FieldType, Data>>(argument_types);
        if (which.isIPv6())
            return std::make_shared<AggregateFunctionUniq<DataTypeIPv6::FieldType, Data>>(argument_types);
        if (which.isTuple())
        {
            if (use_exact_hash_function)
                return std::make_shared<AggregateFunctionUniqVariadic<DataForVariadic<true, true>>>(argument_types);
            return std::make_shared<AggregateFunctionUniqVariadic<DataForVariadic<false, true>>>(argument_types);
        }
    }

    /// "Variadic" method also works as a fallback generic case for single argument.
    if (use_exact_hash_function)
        return std::make_shared<AggregateFunctionUniqVariadic<DataForVariadic<true, false>>>(argument_types);
    return std::make_shared<AggregateFunctionUniqVariadic<DataForVariadic<false, false>>>(argument_types);
}

template <bool is_exact, template <typename, bool> typename Data, template <bool, bool, bool> typename DataForVariadic, bool is_able_to_parallelize_merge>
AggregateFunctionPtr
createAggregateFunctionUniq(const std::string & name, const DataTypes & argument_types, const Array & params, const Settings *)
{
    assertNoParameters(name, params);

    if (argument_types.empty())
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Incorrect number of arguments for aggregate function {}", name);

    /// We use exact hash function if the user wants it;
    /// or if the arguments are not contiguous in memory, because only exact hash function have support for this case.
    bool use_exact_hash_function = is_exact || !isAllArgumentsContiguousInMemory(argument_types);

    if (argument_types.size() == 1)
    {
        const IDataType & argument_type = *argument_types[0];

        AggregateFunctionPtr res(createWithNumericType<AggregateFunctionUniq, Data, is_able_to_parallelize_merge>(*argument_types[0], argument_types));

        WhichDataType which(argument_type);
        if (res)
            return res;
        if (which.isDate())
            return std::make_shared<
                AggregateFunctionUniq<DataTypeDate::FieldType, Data<DataTypeDate::FieldType, is_able_to_parallelize_merge>>>(
                argument_types);
        if (which.isDate32())
            return std::make_shared<
                AggregateFunctionUniq<DataTypeDate32::FieldType, Data<DataTypeDate32::FieldType, is_able_to_parallelize_merge>>>(
                argument_types);
        if (which.isDateTime())
            return std::make_shared<
                AggregateFunctionUniq<DataTypeDateTime::FieldType, Data<DataTypeDateTime::FieldType, is_able_to_parallelize_merge>>>(
                argument_types);
        if (which.isStringOrFixedString())
            return std::make_shared<AggregateFunctionUniq<String, Data<String, is_able_to_parallelize_merge>>>(argument_types);
        if (which.isUUID())
            return std::make_shared<
                AggregateFunctionUniq<DataTypeUUID::FieldType, Data<DataTypeUUID::FieldType, is_able_to_parallelize_merge>>>(
                argument_types);
        if (which.isIPv4())
            return std::make_shared<
                AggregateFunctionUniq<DataTypeIPv4::FieldType, Data<DataTypeIPv4::FieldType, is_able_to_parallelize_merge>>>(
                argument_types);
        if (which.isIPv6())
            return std::make_shared<
                AggregateFunctionUniq<DataTypeIPv6::FieldType, Data<DataTypeIPv6::FieldType, is_able_to_parallelize_merge>>>(
                argument_types);
        if (which.isTuple())
        {
            if (use_exact_hash_function)
                return std::make_shared<AggregateFunctionUniqVariadic<DataForVariadic<true, true, is_able_to_parallelize_merge>>>(
                    argument_types);
            return std::make_shared<AggregateFunctionUniqVariadic<DataForVariadic<false, true, is_able_to_parallelize_merge>>>(
                argument_types);
        }
    }

    /// "Variadic" method also works as a fallback generic case for single argument.
    if (use_exact_hash_function)
        return std::make_shared<AggregateFunctionUniqVariadic<DataForVariadic<true, false, is_able_to_parallelize_merge>>>(argument_types);
    return std::make_shared<AggregateFunctionUniqVariadic<DataForVariadic<false, false, is_able_to_parallelize_merge>>>(argument_types);
}

}

void registerAggregateFunctionsUniq(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = { .returns_default_when_only_null = true, .is_order_dependent = false };

    /// uniq documentation
    FunctionDocumentation::Description description_uniq = R"(
Calculates the approximate number of different values of the argument.

The function uses an adaptive sampling algorithm. For the calculation state, the function uses a sample of element hash values up to 65536. This algorithm is very accurate and very efficient on the CPU. When the query contains several of these functions, using uniq is almost as fast as using other aggregate functions.

<details>
<summary>Implementation details</summary>

This function calculates a hash for all parameters in the aggregate, then uses it in calculations.
It uses an adaptive sampling algorithm.
For the calculation state, the function uses a sample of element hash values up to 65536.
This algorithm is very accurate and very efficient on the CPU.
When the query contains several of these functions, using `uniq` is almost as fast as using other aggregate functions.

</details>

:::tip
We recommend using this function over other variants in almost all scenarios.
:::
    )";
    FunctionDocumentation::Syntax syntax_uniq = R"(
uniq(x[, ...])
    )";
    FunctionDocumentation::Arguments arguments_uniq = {
        {"x", "The function takes a variable number of parameters.", {"Tuple(T)", "Array(T)", "Date", "DateTime", "String", "(U)Int*", "Float*", "Decimal"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_uniq = {"Returns a UInt64-type number representing the approximate number of different values.", {"UInt64"}};
    FunctionDocumentation::Examples examples_uniq = {
    {
        "Example usage",
        R"(
CREATE TABLE example_table (
    id UInt32,
    category String,
    value Float64
) ENGINE = Memory;

INSERT INTO example_table VALUES
(1, 'A', 10.5),
(2, 'B', 20.3),
(3, 'A', 15.7),
(4, 'C', 8.9),
(5, 'B', 12.1),
(6, 'A', 18.4);

SELECT uniq(category) as unique_categories
FROM example_table;
        )",
        R"(
┌─unique_categories─┐
│                 3 │
└───────────────────┘
        )"
    },
    {
        "Multiple arguments",
        R"(
SELECT uniq(category, value) as unique_combinations
FROM example_table;
        )",
        R"(
┌─unique_combinations─┐
│                   6 │
└─────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_uniq = {1, 1};
    FunctionDocumentation::Category category_uniq = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation_uniq = {description_uniq, syntax_uniq, arguments_uniq, {}, returned_value_uniq, examples_uniq, introduced_in_uniq, category_uniq};

    factory.registerFunction("uniq",
        {createAggregateFunctionUniq<AggregateFunctionUniqUniquesHashSetData, AggregateFunctionUniqUniquesHashSetDataForVariadic>, documentation_uniq, properties});

    /// uniqHLL12 documentation
    FunctionDocumentation::Description description_uniqHLL12 = R"(
Calculates the approximate number of different argument values, using the [HyperLogLog](https://en.wikipedia.org/wiki/HyperLogLog) algorithm.

:::warning
We do not recommend using this function. In most cases, use the [uniq](https://clickhouse.com/docs/sql-reference/aggregate-functions/reference/uniq) or [uniqCombined](https://clickhouse.com/docs/sql-reference/aggregate-functions/reference/uniqcombined) function.
:::

<details>
<summary>Implementation details</summary>
This function calculates a hash for all parameters in the aggregate, then uses it in calculations.
It uses the HyperLogLog algorithm to approximate the number of different argument values.

2^12 5-bit cells are used.
The size of the state is slightly more than 2.5 KB.
The result is not very accurate (up to ~10% error) for small data sets (\<10K elements).
However, the result is fairly accurate for high-cardinality data sets (10K-100M), with a maximum error of ~1.6%.
Starting from 100M, the estimation error increases, and the function will return very inaccurate results for data sets with extremely high cardinality (1B+ elements).

Provides a determinate result (it does not depend on the query processing order).
</details>
    )";
    FunctionDocumentation::Syntax syntax_uniqHLL12 = R"(
uniqHLL12(x[, ...])
    )";
    FunctionDocumentation::Arguments arguments_uniqHLL12 = {
        {"x", "The function takes a variable number of parameters.", {"Tuple(T)", "Array(T)", "Date", "DateTime", "String", "(U)Int*", "Float*", "Decimal"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_uniqHLL12 = {"Returns a UInt64-type number representing the approximate number of different argument values.", {"UInt64"}};
    FunctionDocumentation::Examples examples_uniqHLL12 = {
    {
        "Basic usage",
        R"(
CREATE TABLE example_hll
(
    id UInt32,
    category String
)
ENGINE = Memory;

INSERT INTO example_hll VALUES
(1, 'A'), (2, 'B'), (3, 'A'), (4, 'C'), (5, 'B'), (6, 'A');

SELECT uniqHLL12(category) AS hll_unique_categories
FROM example_hll;
        )",
        R"(
┌─hll_unique_categories─┐
│                     3 │
└───────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_uniqHLL12 = {1, 1};
    FunctionDocumentation::Category category_uniqHLL12 = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation_uniqHLL12 = {description_uniqHLL12, syntax_uniqHLL12, arguments_uniqHLL12, {}, returned_value_uniqHLL12, examples_uniqHLL12, introduced_in_uniqHLL12, category_uniqHLL12};

    factory.registerFunction("uniqHLL12",
        {createAggregateFunctionUniq<false, AggregateFunctionUniqHLL12Data, AggregateFunctionUniqHLL12DataForVariadic, false /* is_able_to_parallelize_merge */>, documentation_uniqHLL12, properties});

    auto assign_bool_param = [](const std::string & name, const DataTypes & argument_types, const Array & params, const Settings * settings)
    {
        /// Using two level hash set if we wouldn't be able to merge in parallel can cause ~10% slowdown.
        if (settings && (*settings)[Setting::max_threads] > 1)
            return createAggregateFunctionUniq<
                true, AggregateFunctionUniqExactData, AggregateFunctionUniqExactDataForVariadic, true /* is_able_to_parallelize_merge */>(name, argument_types, params, settings);
        return createAggregateFunctionUniq<
            true,
            AggregateFunctionUniqExactData,
            AggregateFunctionUniqExactDataForVariadic,
            false /* is_able_to_parallelize_merge */>(name, argument_types, params, settings);
    };

    /// uniqExact documentation
    FunctionDocumentation::Description description_uniqExact = R"(
Calculates the exact number of different argument values.

:::warning
The `uniqExact` function uses more memory than `uniq`, because the size of the state has unbounded growth as the number of different values increases.
Use the `uniqExact` function if you absolutely need an exact result.
Otherwise use the [`uniq`](https://clickhouse.com/docs/sql-reference/aggregate-functions/reference/uniq) function.
:::
    )";
    FunctionDocumentation::Syntax syntax_uniqExact = R"(
uniqExact(x[, ...])
    )";
    FunctionDocumentation::Arguments arguments_uniqExact = {
        {"x", "The function takes a variable number of parameters.", {"Tuple(T)", "Array(T)", "Date", "DateTime", "String", "(U)Int*", "Float*", "Decimal"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_uniqExact = {"Returns the exact number of different argument values as a UInt64.", {"UInt64"}};
    FunctionDocumentation::Examples examples_uniqExact = {
    {
        "Basic usage",
        R"(
CREATE TABLE example_data
(
    id UInt32,
    category String
)
ENGINE = Memory;

INSERT INTO example_data VALUES
(1, 'A'), (2, 'B'), (3, 'A'), (4, 'C'), (5, 'B'), (6, 'A');

SELECT uniqExact(category) as exact_unique_categories
FROM example_data;
        )",
        R"(
┌─exact_unique_categories─┐
│                       3 │
└─────────────────────────┘
        )"
    },
    {
        "Multiple arguments",
        R"(
SELECT uniqExact(id, category) as exact_unique_combinations
FROM example_data;
        )",
        R"(
┌─exact_unique_combinations─┐
│                         6 │
└───────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_uniqExact = {1, 1};
    FunctionDocumentation::Category category_uniqExact = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation_uniqExact = {description_uniqExact, syntax_uniqExact, arguments_uniqExact, {}, returned_value_uniqExact, examples_uniqExact, introduced_in_uniqExact, category_uniqExact};

    factory.registerFunction("uniqExact", {assign_bool_param, documentation_uniqExact, properties});

#if USE_DATASKETCHES
    /// uniqTheta documentation
    FunctionDocumentation::Description description_uniqTheta = R"(
Calculates the approximate number of different argument values, using the [Theta Sketch Framework](https://datasketches.apache.org/docs/Theta/ThetaSketches.html#theta-sketch-framework).

<details>
<summary>Implementation details</summary>

This function calculates a hash for all parameters in the aggregate, then uses it in calculations.
It uses the [KMV](https://datasketches.apache.org/docs/Theta/InverseEstimate.html) algorithm to approximate the number of different argument values.

4096(2^12) 64-bit sketch are used.
The size of the state is about 41 KB.

The relative error is 3.125% (95% confidence), see the [relative error table](https://datasketches.apache.org/docs/Theta/ThetaErrorTable.html) for detail.
</details>
    )";
    FunctionDocumentation::Syntax syntax_uniqTheta = R"(
uniqTheta(x[, ...])
    )";
    FunctionDocumentation::Arguments arguments_uniqTheta = {
        {"x", "The function takes a variable number of parameters.", {"Tuple(T)", "Array(T)", "Date", "DateTime", "String", "(U)Int*", "Float*", "Decimal"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_uniqTheta = {"Returns a UInt64-type number representing the approximate number of different argument values.", {"UInt64"}};
    FunctionDocumentation::Examples examples_uniqTheta = {
    {
        "Basic usage",
        R"(
CREATE TABLE example_theta
(
    id UInt32,
    category String
)
ENGINE = Memory;

INSERT INTO example_theta VALUES
(1, 'A'), (2, 'B'), (3, 'A'), (4, 'C'), (5, 'B'), (6, 'A');

SELECT uniqTheta(category) as theta_unique_categories
FROM example_theta;
        )",
        R"(
┌─theta_unique_categories─┐
│                       3 │
└─────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_uniqTheta = {21, 6};
    FunctionDocumentation::Category category_uniqTheta = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation_uniqTheta = {description_uniqTheta, syntax_uniqTheta, arguments_uniqTheta, {}, returned_value_uniqTheta, examples_uniqTheta, introduced_in_uniqTheta, category_uniqTheta};

    factory.registerFunction("uniqTheta",
        {createAggregateFunctionUniq<AggregateFunctionUniqThetaData, AggregateFunctionUniqThetaDataForVariadic>, documentation_uniqTheta, properties});
#endif

}

}
