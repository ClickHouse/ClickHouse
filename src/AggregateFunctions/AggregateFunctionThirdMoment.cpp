#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionStatisticsSimple.h>


namespace DB
{

template <typename T> using AggregateFunctionThirdMoment = AggregateFunctionVarianceSimple<StatFuncOneArg<T, 3>>;

void registerAggregateFunctionsStatisticsThirdMoment(AggregateFunctionFactory & factory)
{

    FunctionDocumentation::Description description_skewSamp = R"(
Computes the [sample skewness](https://en.wikipedia.org/wiki/Skewness) of a sequence.

It represents an unbiased estimate of the skewness of a random variable if passed values form its sample.
    )";
    FunctionDocumentation::Syntax syntax_skewSamp = R"(
skewSamp(expr)
    )";
    FunctionDocumentation::Parameters parameters_skewSamp = {};
    FunctionDocumentation::Arguments arguments_skewSamp = {
        {"expr", "An expression returning a number.", {"Expression"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_skewSamp = {"Returns the skewness of the given distribution. If `n <= 1` (`n` is the size of the sample), then the function returns `nan`.", {"Float64"}};
    FunctionDocumentation::Examples examples_skewSamp = {
    {
        "Symmetric distribution",
        R"(
SELECT skewSamp(number) FROM numbers(100);
        )",
        R"(
┌─skewSamp(number)─┐
│                0 │
└──────────────────┘
        )"
    },
    {
        "Right-skewed distribution",
        R"(
SELECT skewSamp(x) FROM (SELECT pow(number, 2) AS x FROM numbers(10));
        )",
        R"(
┌────────skewSamp(x)─┐
│ 0.5751042382747413 │
└────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_skewSamp = {20, 1};
    FunctionDocumentation::Category category_skewSamp = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation_skewSamp = {description_skewSamp, syntax_skewSamp, arguments_skewSamp, parameters_skewSamp, returned_value_skewSamp, examples_skewSamp, introduced_in_skewSamp, category_skewSamp};

    factory.registerFunction("skewSamp", {createAggregateFunctionStatisticsUnary<AggregateFunctionThirdMoment, StatisticsFunctionKind::skewSamp>, documentation_skewSamp});

    FunctionDocumentation::Description description_skewPop = R"(
Computes the [skewness](https://en.wikipedia.org/wiki/Skewness) of a sequence.
    )";
    FunctionDocumentation::Syntax syntax_skewPop = R"(
skewPop(expr)
    )";
    FunctionDocumentation::Parameters parameters_skewPop = {};
    FunctionDocumentation::Arguments arguments_skewPop = {
        {"expr", "An expression returning a number.", {"Expression"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_skewPop = {"Returns the skewness of the given distribution.", {"Float64"}};
    FunctionDocumentation::Examples examples_skewPop = {
    {
        "Symmetric distribution",
        R"(
SELECT skewPop(number) FROM numbers(100);
        )",
        R"(
┌─skewPop(number)─┐
│               0 │
└─────────────────┘
        )"
    },
    {
        "Right-skewed distribution",
        R"(
SELECT skewPop(x) FROM (SELECT pow(number, 2) AS x FROM numbers(10));
        )",
        R"(
┌─────────skewPop(x)─┐
│ 0.6735701055423582 │
└────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_skewPop = {20, 1};
    FunctionDocumentation::Category category_skewPop = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation_skewPop = {description_skewPop, syntax_skewPop, arguments_skewPop, parameters_skewPop, returned_value_skewPop, examples_skewPop, introduced_in_skewPop, category_skewPop};

    factory.registerFunction("skewPop", {createAggregateFunctionStatisticsUnary<AggregateFunctionThirdMoment, StatisticsFunctionKind::skewPop>, documentation_skewPop});
}

}
