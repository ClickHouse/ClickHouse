#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionStatisticsSimple.h>


namespace DB
{

template <typename T> using AggregateFunctionSecondMoment = AggregateFunctionVarianceSimple<StatFuncOneArg<T, 2>>;

void registerAggregateFunctionsStatisticsSecondMoment(AggregateFunctionFactory & factory)
{
    factory.registerFunction("varSamp", createAggregateFunctionStatisticsUnary<AggregateFunctionSecondMoment, StatisticsFunctionKind::varSamp>);
    factory.registerFunction("varPop", createAggregateFunctionStatisticsUnary<AggregateFunctionSecondMoment, StatisticsFunctionKind::varPop>);
    FunctionDocumentation::Description description_stddevSamp = R"(
Returns the sample standard deviation of a numeric data sequence.
The result is equal to the square root of [`varSamp`](/sql-reference/aggregate-functions/reference/varsamp).

:::note
This function uses a numerically unstable algorithm.
If you need [numerical stability](https://en.wikipedia.org/wiki/Numerical_stability) in calculations, use the [`stddevSampStable`](/sql-reference/aggregate-functions/reference/stddevsampstable) function. It works slower but provides a lower computational error.
:::
    )";
    FunctionDocumentation::Syntax syntax_stddevSamp = R"(
stddevSamp(x)
    )";
    FunctionDocumentation::Arguments arguments_stddevSamp = {
        {"x", "Values for which to find the square root of sample variance.", {"(U)Int*", "Float*", "Decimal*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_stddevSamp = {"Returns the square root of sample variance of `x`.", {"Float64"}};
    FunctionDocumentation::Examples examples_stddevSamp = {
    {
        "Computing sample standard deviation",
        R"(
DROP TABLE IF EXISTS test_data;
CREATE TABLE test_data
(
    population UInt8,
)
ENGINE = Log;

INSERT INTO test_data VALUES (3),(3),(3),(4),(4),(5),(5),(7),(11),(15);

SELECT
    stddevSamp(population)
FROM test_data;
        )",
        R"(
┌─stddevSamp(population)─┐
│                      4 │
└────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_stddevSamp = {1, 1};
    FunctionDocumentation::Category category_stddevSamp = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation_stddevSamp = {description_stddevSamp, syntax_stddevSamp, arguments_stddevSamp, {}, returned_value_stddevSamp, examples_stddevSamp, introduced_in_stddevSamp, category_stddevSamp};

    factory.registerFunction("stddevSamp", {createAggregateFunctionStatisticsUnary<AggregateFunctionSecondMoment, StatisticsFunctionKind::stddevSamp>, {}, documentation_stddevSamp});
    FunctionDocumentation::Description description_stddevPop = R"(
Returns the population standard deviation of a numeric data sequence.
The result is equal to the square root of [`varPop`](/sql-reference/aggregate-functions/reference/varpop).

:::note
This function uses a numerically unstable algorithm. If you need [numerical stability](https://en.wikipedia.org/wiki/Numerical_stability) in calculations, use the [`stddevPopStable`](/sql-reference/aggregate-functions/reference/stddevpopstable) function. It works slower but provides a lower computational error.
:::
    )";
    FunctionDocumentation::Syntax syntax_stddevPop = R"(
stddevPop(x)
    )";
    FunctionDocumentation::Arguments arguments_stddevPop = {
        {"x", "Population of values to find the standard deviation of.", {"(U)Int*", "Float*", "Decimal*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_stddevPop = {"Returns the square root of population variance of `x`.", {"Float64"}};
    FunctionDocumentation::Examples examples_stddevPop = {
    {
        "Computing population standard deviation",
        R"(
CREATE TABLE test_data (population UInt8) ENGINE = Log;
INSERT INTO test_data VALUES (3),(3),(3),(4),(4),(5),(5),(7),(11),(15);

SELECT stddevPop(population) AS stddev FROM test_data;
        )",
        R"(
┌────────────stddev─┐
│ 3.794733192202055 │
└───────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_stddevPop = {1, 1};
    FunctionDocumentation::Category category_stddevPop = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation_stddevPop = {description_stddevPop, syntax_stddevPop, arguments_stddevPop, {}, returned_value_stddevPop, examples_stddevPop, introduced_in_stddevPop, category_stddevPop};

    factory.registerFunction("stddevPop", {createAggregateFunctionStatisticsUnary<AggregateFunctionSecondMoment, StatisticsFunctionKind::stddevPop>, {}, documentation_stddevPop});

    /// Synonyms for compatibility.
    factory.registerAlias("VAR_SAMP", "varSamp", AggregateFunctionFactory::Case::Insensitive);
    factory.registerAlias("VAR_POP", "varPop", AggregateFunctionFactory::Case::Insensitive);
    factory.registerAlias("STDDEV_SAMP", "stddevSamp", AggregateFunctionFactory::Case::Insensitive);
    factory.registerAlias("STDDEV_POP", "stddevPop", AggregateFunctionFactory::Case::Insensitive);
    factory.registerAlias("STD", "stddevPop", AggregateFunctionFactory::Case::Insensitive);
}

}
