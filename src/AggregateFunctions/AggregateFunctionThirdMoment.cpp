#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionStatisticsSimple.h>


namespace DB
{

template <typename T> using AggregateFunctionThirdMoment = AggregateFunctionVarianceSimple<StatFuncOneArg<T, 3>>;

void registerAggregateFunctionsStatisticsThirdMoment(AggregateFunctionFactory & factory)
{
    FunctionDocumentation::Description description_samp = R"(
Computes the [sample skewness](https://en.wikipedia.org/wiki/Skewness) of a sequence.

It represents an unbiased estimate of the skewness of a random variable if passed values form its sample.
    )";
    FunctionDocumentation::Syntax syntax_samp = R"(
skewSamp(expr)
    )";
    FunctionDocumentation::Arguments arguments_samp = {
        {"expr", "Expression returning a number.", {"(U)Int*", "Float*", "Decimal*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_samp = {"Returns the sample skewness of the given distribution. If `n <= 1` (`n` is the size of the sample), then the function returns `nan`.", {"Float64"}};
    FunctionDocumentation::Examples examples_samp = {
    {
        "Computing sample skewness of a series",
        R"(
CREATE TABLE series_with_value_column (value Float64) ENGINE = Memory;
INSERT INTO series_with_value_column VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10);

SELECT skewSamp(value) FROM series_with_value_column;
        )",
        R"(
┌─skewSamp(value)─┐
│               0 │
└─────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_samp = {20, 1};
    FunctionDocumentation::Category category_samp = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation_samp = {description_samp, syntax_samp, arguments_samp, {}, returned_value_samp, examples_samp, introduced_in_samp, category_samp};

    factory.registerFunction("skewSamp", {createAggregateFunctionStatisticsUnary<AggregateFunctionThirdMoment, StatisticsFunctionKind::skewSamp>, {}, documentation_samp});

    FunctionDocumentation::Description description = R"(
Computes the [skewness](https://en.wikipedia.org/wiki/Skewness) of a sequence.

Skewness is a measure of the asymmetry of the probability distribution of a real-valued random variable about its mean.
    )";
    FunctionDocumentation::Syntax syntax = R"(
skewPop(expr)
    )";
    FunctionDocumentation::Arguments arguments = {
        {"expr", "Expression returning a number.", {"(U)Int*", "Float*", "Decimal*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"The skewness of the given distribution.", {"Float64"}};
    FunctionDocumentation::Examples examples = {
    {
        "Computing skewness of a series",
        R"(
CREATE TABLE series_with_value_column (value Float64) ENGINE = Memory;
INSERT INTO series_with_value_column VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10);

SELECT skewPop(value) FROM series_with_value_column;
        )",
        R"(
┌─skewPop(value)─┐
│              0 │
└────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction("skewPop", {createAggregateFunctionStatisticsUnary<AggregateFunctionThirdMoment, StatisticsFunctionKind::skewPop>, {}, documentation});
}

}
