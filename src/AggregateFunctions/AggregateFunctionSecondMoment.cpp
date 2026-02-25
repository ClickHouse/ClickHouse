#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionStatisticsSimple.h>


namespace DB
{

template <typename T> using AggregateFunctionSecondMoment = AggregateFunctionVarianceSimple<StatFuncOneArg<T, 2>>;

void registerAggregateFunctionsStatisticsSecondMoment(AggregateFunctionFactory & factory)
{
    /// varSamp documentation
    FunctionDocumentation::Description description_varSamp = R"(
Calculate the sample variance of a data set.

The sample variance is calculated using the formula:

$$
\frac{\Sigma{(x - \bar{x})^2}}{n-1}
$$

<br/>

Where:
- $x$ is each individual data point in the data set
- $\bar{x}$ is the arithmetic mean of the data set
- $n$ is the number of data points in the data set

The function assumes that the input data set represents a sample from a larger population. If you want to calculate the variance of the entire population (when you have the complete data set), you should use [`varPop`](/sql-reference/aggregate-functions/reference/varPop) instead.

:::note
This function uses a numerically unstable algorithm. If you need [numerical stability](https://en.wikipedia.org/wiki/Numerical_stability) in calculations, use the [`varSampStable`](/sql-reference/aggregate-functions/reference/varsampstable) function. It works slower but provides a lower computational error.
:::
    )";
    FunctionDocumentation::Syntax syntax_varSamp = R"(
varSamp(x)
    )";
    FunctionDocumentation::Arguments arguments_varSamp = {
        {"x", "The population for which you want to calculate the sample variance.", {"(U)Int*", "Float*", "Decimal*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_varSamp = {"Returns the sample variance of the input data set `x`.", {"Float64"}};
    FunctionDocumentation::Examples examples_varSamp = {
    {
        "Computing sample variance",
        R"(
DROP TABLE IF EXISTS test_data;
CREATE TABLE test_data
(
    x Float64
)
ENGINE = Memory;

INSERT INTO test_data VALUES (10.5), (12.3), (9.8), (11.2), (10.7);

SELECT round(varSamp(x),3) AS var_samp FROM test_data;
        )",
        R"(
┌─var_samp─┐
│    0.865 │
└──────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_varSamp = {1, 1};
    FunctionDocumentation::Category category_varSamp = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation_varSamp = {description_varSamp, syntax_varSamp, arguments_varSamp, {}, returned_value_varSamp, examples_varSamp, introduced_in_varSamp, category_varSamp};

    factory.registerFunction("varSamp", {createAggregateFunctionStatisticsUnary<AggregateFunctionSecondMoment, StatisticsFunctionKind::varSamp>, documentation_varSamp});

    /// varPop documentation
    FunctionDocumentation::Description description_varPop = R"(
Calculates the population variance.

The population variance is calculated using the formula:

$$
\frac{\Sigma{(x - \bar{x})^2}}{n}
$$

<br/>

Where:
- $x$ is each value in the population
- $\bar{x}$ is the population mean
- $n$ is the population size

:::note
This function uses a numerically unstable algorithm. If you need [numerical stability](https://en.wikipedia.org/wiki/Numerical_stability) in calculations, use the [`varPopStable`](https://clickhouse.com/docs/sql-reference/aggregate-functions/reference/varpopstable) function. It works slower but provides a lower computational error.
:::
    )";
    FunctionDocumentation::Syntax syntax_varPop = R"(
varPop(x)
    )";
    FunctionDocumentation::Arguments arguments_varPop = {
        {"x", "Population of values to find the population variance of.", {"(U)Int*", "Float*", "Decimal*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_varPop = {"Returns the population variance of `x`.", {"Float64"}};
    FunctionDocumentation::Examples examples_varPop = {
    {
        "Computing population variance",
        R"(
DROP TABLE IF EXISTS test_data;
CREATE TABLE test_data
(
    x UInt8,
)
ENGINE = Memory;

INSERT INTO test_data VALUES (3), (3), (3), (4), (4), (5), (5), (7), (11), (15);

SELECT
    varPop(x) AS var_pop
FROM test_data;
        )",
        R"(
┌─var_pop─┐
│    14.4 │
└─────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_varPop = {1, 1};
    FunctionDocumentation::Category category_varPop = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation_varPop = {description_varPop, syntax_varPop, arguments_varPop, {}, returned_value_varPop, examples_varPop, introduced_in_varPop, category_varPop};

    factory.registerFunction("varPop", {createAggregateFunctionStatisticsUnary<AggregateFunctionSecondMoment, StatisticsFunctionKind::varPop>, documentation_varPop});
    FunctionDocumentation::Description description_stddevSamp = R"(
Returns the sample standard deviation of a numeric data sequence.
The result is equal to the square root of [`varSamp`](/sql-reference/aggregate-functions/reference/varSamp).

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

    factory.registerFunction("stddevSamp", {createAggregateFunctionStatisticsUnary<AggregateFunctionSecondMoment, StatisticsFunctionKind::stddevSamp>, documentation_stddevSamp});
    FunctionDocumentation::Description description_stddevPop = R"(
Returns the population standard deviation of a numeric data sequence.
The result is equal to the square root of [`varPop`](/sql-reference/aggregate-functions/reference/varPop).

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

    factory.registerFunction("stddevPop", {createAggregateFunctionStatisticsUnary<AggregateFunctionSecondMoment, StatisticsFunctionKind::stddevPop>, documentation_stddevPop});

    /// Synonyms for compatibility.
    factory.registerAlias("VAR_SAMP", "varSamp", AggregateFunctionFactory::Case::Insensitive);
    factory.registerAlias("VAR_POP", "varPop", AggregateFunctionFactory::Case::Insensitive);
    factory.registerAlias("STDDEV_SAMP", "stddevSamp", AggregateFunctionFactory::Case::Insensitive);
    factory.registerAlias("STDDEV_POP", "stddevPop", AggregateFunctionFactory::Case::Insensitive);
    factory.registerAlias("STD", "stddevPop", AggregateFunctionFactory::Case::Insensitive);
}

}
