#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionSum.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>


namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

template <typename T>
struct SumSimple
{
    /// @note It uses slow Decimal128/256 (cause we need such a variant). sumWithOverflow is faster for Decimal32/64
    using ResultType = std::conditional_t<is_decimal<T>,
                                        std::conditional_t<std::is_same_v<T, Decimal256>, Decimal256, Decimal128>,
                                        NearestFieldType<T>>;
    using AggregateDataType = AggregateFunctionSumData<ResultType>;
    using Function = AggregateFunctionSum<T, ResultType, AggregateDataType, AggregateFunctionTypeSum>;
};

template <typename T>
struct SumSameType
{
    using ResultType = T;
    using AggregateDataType = AggregateFunctionSumData<ResultType>;
    using Function = AggregateFunctionSum<T, ResultType, AggregateDataType, AggregateFunctionTypeSumWithOverflow>;
};

template <typename T>
struct SumKahan
{
    using ResultType = Float64;
    using AggregateDataType = AggregateFunctionSumKahanData<ResultType>;
    using Function = AggregateFunctionSum<T, ResultType, AggregateDataType, AggregateFunctionTypeSumKahan>;
};

template <typename T> using AggregateFunctionSumSimple = typename SumSimple<T>::Function;
template <typename T> using AggregateFunctionSumWithOverflow = typename SumSameType<T>::Function;
template <typename T> using AggregateFunctionSumKahan =
    std::conditional_t<is_decimal<T>, typename SumSimple<T>::Function, typename SumKahan<T>::Function>;


template <template <typename> class Function>
AggregateFunctionPtr createAggregateFunctionSum(const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertNoParameters(name, parameters);
    assertUnary(name, argument_types);

    AggregateFunctionPtr res;
    const DataTypePtr & data_type = argument_types[0];
    if (isDecimal(data_type))
        res.reset(createWithDecimalType<Function>(*data_type, *data_type, argument_types));
    else
        res.reset(createWithNumericType<Function>(*data_type, argument_types));

    if (!res)
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument for aggregate function {}",
                        argument_types[0]->getName(), name);
    return res;
}

}

void registerAggregateFunctionSum(AggregateFunctionFactory & factory)
{
    FunctionDocumentation::Description description = R"(
Calculates the sum of numeric values.
    )";
    FunctionDocumentation::Syntax syntax = R"(
sum(num)
    )";
    FunctionDocumentation::Arguments arguments = {
        {"num", "Column of numeric values.", {"(U)Int*", "Float*", "Decimal*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {
        "Returns the sum of the values.", {"(U)Int*", "Float*", "Decimal*"}
    };
    FunctionDocumentation::Examples examples = {
    {
        "Computing sum of employee salaries",
        R"(
CREATE TABLE employees
(
    id UInt32,
    name String,
    salary UInt32
)
ENGINE = Memory;

INSERT INTO employees VALUES
    (87432, 'John Smith', 45680),
    (59018, 'Jane Smith', 72350),
    (20376, 'Ivan Ivanovich', 58900),
    (71245, 'Anastasia Ivanovna', 89210);

SELECT sum(salary) FROM employees;
        )",
        R"(
┌─sum(salary)─┐
│      266140 │
└─────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction("sum", {createAggregateFunctionSum<AggregateFunctionSumSimple>, documentation}, AggregateFunctionFactory::Case::Insensitive);

    FunctionDocumentation::Description description_overflow = R"(
Computes a sum of numeric values, using the same data type for the result as for the input parameters.
If the sum exceeds the maximum value for this data type, it is calculated with overflow.
    )";
    FunctionDocumentation::Syntax syntax_overflow = R"(
sumWithOverflow(num)
    )";
    FunctionDocumentation::Arguments arguments_overflow = {
        {"num", "Column of numeric values.", {"(U)Int*", "Float*", "Decimal*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_overflow = {
        "The sum of the values.", {"(U)Int*", "Float*", "Decimal*"}
    };
    FunctionDocumentation::Examples examples_overflow = {
    {
        "Demonstrating overflow behavior with UInt16",
        R"(
CREATE TABLE employees
(
    id UInt32,
    name String,
    monthly_salary UInt16 -- selected so that the sum of values produces an overflow
)
ENGINE = Memory;

INSERT INTO employees VALUES
    (1, 'John', 20000),
    (2, 'Jane', 18000),
    (3, 'Bob', 12000),
    (4, 'Alice', 10000),
    (5, 'Charlie', 8000);

-- Query for the total amount of the employee salaries using the sum and sumWithOverflow functions and show their types using the toTypeName function
-- For the sum function the resulting type is UInt64, big enough to contain the sum, whilst for sumWithOverflow the resulting type remains as UInt16.

SELECT
    sum(monthly_salary) AS no_overflow,
    sumWithOverflow(monthly_salary) AS overflow,
    toTypeName(no_overflow),
    toTypeName(overflow)
FROM employees;
        )",
        R"(
┌─no_overflow─┬─overflow─┬─toTypeName(no_overflow)─┬─toTypeName(overflow)─┐
│       68000 │     2464 │ UInt64                  │ UInt16               │
└─────────────┴──────────┴─────────────────────────┴──────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_overflow = {1, 1};
    FunctionDocumentation::Category category_overflow = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation_overflow = {description_overflow, syntax_overflow, arguments_overflow, {}, returned_value_overflow, examples_overflow, introduced_in_overflow, category_overflow};

    factory.registerFunction("sumWithOverflow", {createAggregateFunctionSum<AggregateFunctionSumWithOverflow>, documentation_overflow});

    FunctionDocumentation::Description description_kahan = R"(
Calculates the sum of the numbers with [Kahan compensated summation algorithm](https://en.wikipedia.org/wiki/Kahan_summation_algorithm).
Slower than [`sum`](/sql-reference/aggregate-functions/reference/sum) function.
The compensation works only for [Float](/sql-reference/data-types/float) types.
    )";
    FunctionDocumentation::Syntax syntax_kahan = R"(
sumKahan(x)
    )";
    FunctionDocumentation::Arguments arguments_kahan = {
        {"x", "Input value.", {"Integer", "Float", "Decimal"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_kahan = {
        "Returns the sum of numbers.", {"(U)Int*", "Float*", "Decimal"}
    };
    FunctionDocumentation::Examples examples_kahan = {
    {
        "Demonstrating precision improvement with Kahan summation",
        R"(
SELECT sum(0.1), sumKahan(0.1) FROM numbers(10);
        )",
        R"(
┌───────────sum(0.1)─┬─sumKahan(0.1)─┐
│ 0.9999999999999999 │             1 │
└────────────────────┴───────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_kahan = {1, 1};
    FunctionDocumentation::Category category_kahan = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation_kahan = {description_kahan, syntax_kahan, arguments_kahan, {}, returned_value_kahan, examples_kahan, introduced_in_kahan, category_kahan};

    factory.registerFunction("sumKahan", {createAggregateFunctionSum<AggregateFunctionSumKahan>, documentation_kahan});
}

}
