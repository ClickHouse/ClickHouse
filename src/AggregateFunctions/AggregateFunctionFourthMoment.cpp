#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionStatisticsSimple.h>


namespace DB
{

template <typename T> using AggregateFunctionFourthMoment = AggregateFunctionVarianceSimple<StatFuncOneArg<T, 4>>;

void registerAggregateFunctionsStatisticsFourthMoment(AggregateFunctionFactory & factory)
{
    FunctionDocumentation::Description description_samp = R"(
Computes the [sample kurtosis](https://en.wikipedia.org/wiki/Kurtosis) of a sequence.

It represents an unbiased estimate of the kurtosis of a random variable if passed values form its sample.
    )";
    FunctionDocumentation::Syntax syntax_samp = R"(
kurtSamp(expr)
    )";
    FunctionDocumentation::Arguments arguments_samp = {
        {"expr", "[Expression](/sql-reference/syntax#expressions) returning a number.", {"(U)Int*", "Float*", "Decimal"}}
    };
    FunctionDocumentation::Parameters parameters = {};
    FunctionDocumentation::ReturnedValue returned_value_samp = {"Returns the kurtosis of the given distribution. If `n <= 1` (`n` is a size of the sample), then the function returns `nan`.", {"Float64"}};
    FunctionDocumentation::Examples examples_samp = {
    {
        "Computing sample kurtosis",
        R"(
CREATE TABLE test_data (x Float64) ENGINE = Memory;
INSERT INTO test_data VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10);

SELECT kurtSamp(x) FROM test_data;
        )",
        R"(
┌────────kurtSamp(x)─┐
│ 1.4383636363636365 │
└────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_samp = {20, 1};
    FunctionDocumentation::Category category_samp = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation_samp = {description_samp, syntax_samp, arguments_samp, {}, returned_value_samp, examples_samp, introduced_in_samp, category_samp};

    factory.registerFunction("kurtSamp", {createAggregateFunctionStatisticsUnary<AggregateFunctionFourthMoment, StatisticsFunctionKind::kurtSamp>, documentation_samp});

    FunctionDocumentation::Description description = R"(
Computes the [kurtosis](https://en.wikipedia.org/wiki/Kurtosis) of a sequence.
    )";
    FunctionDocumentation::Syntax syntax = R"(
kurtPop(expr)
    )";
    FunctionDocumentation::Arguments arguments = {
        {"expr", "[Expression](/sql-reference/syntax#expressions) returning a number.", {"(U)Int*", "Float*", "Decimal"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the kurtosis of the given distribution.", {"Float64"}};
    FunctionDocumentation::Examples examples = {
    {
        "Computing kurtosis",
        R"(
CREATE TABLE test_data (x Float64) ENGINE = Memory;
INSERT INTO test_data VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10);

SELECT kurtPop(x) FROM test_data;
        )",
        R"(
┌─────────kurtPop(x)─┐
│ 1.7757575757575756 │
└────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation = {description, syntax, arguments, parameters, returned_value, examples, introduced_in, category};

    factory.registerFunction("kurtPop", {createAggregateFunctionStatisticsUnary<AggregateFunctionFourthMoment, StatisticsFunctionKind::kurtPop>, documentation});
}

}
