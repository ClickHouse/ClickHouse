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

    factory.registerFunction("sum", {createAggregateFunctionSum<AggregateFunctionSumSimple>, AggregateFunctionFactory::Case::Insensitive, documentation});
    factory.registerFunction("sumWithOverflow", createAggregateFunctionSum<AggregateFunctionSumWithOverflow>);
    factory.registerFunction("sumKahan", createAggregateFunctionSum<AggregateFunctionSumKahan>);
}

}
