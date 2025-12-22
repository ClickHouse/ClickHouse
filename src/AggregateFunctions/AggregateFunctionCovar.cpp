#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionStatisticsSimple.h>


namespace DB
{

template <typename T1, typename T2> using AggregateFunctionCovar = AggregateFunctionVarianceSimple<StatFuncTwoArg<T1, T2, CovarMoments>>;

void registerAggregateFunctionsStatisticsCovar(AggregateFunctionFactory & factory)
{
    factory.registerFunction("covarSamp", createAggregateFunctionStatisticsBinary<AggregateFunctionCovar, StatisticsFunctionKind::covarSamp>);

    FunctionDocumentation::Description covarPop_description = R"(
Calculates the population covariance:

$$\frac{\Sigma{(x - \bar{x})(y - \bar{y})}}{n}$$

:::note
This function uses a numerically unstable algorithm. If you need [numerical stability](https://en.wikipedia.org/wiki/Numerical_stability) in calculations, use the [`covarPopStable`](../reference/covarpopstable.md) function. It works slower but provides a lower computational error.
:::
    )";
    FunctionDocumentation::Syntax covarPop_syntax = "covarPop(x, y)";
    FunctionDocumentation::Arguments covarPop_arguments = {
        {"x", "First variable.", {"(U)Int*", "Float*", "Decimal"}},
        {"y", "Second variable.", {"(U)Int*", "Float*", "Decimal"}}
    };
    FunctionDocumentation::Parameters covarPop_parameters = {};
    FunctionDocumentation::ReturnedValue covarPop_returned_value = {"Returns the population covariance between `x` and `y`.", {"Float64"}};
    FunctionDocumentation::Examples covarPop_examples = {
    {
        "Basic population covariance calculation",
        R"(
DROP TABLE IF EXISTS series;
CREATE TABLE series(i UInt32, x_value Float64, y_value Float64) ENGINE = Memory;
INSERT INTO series(i, x_value, y_value) VALUES (1, 5.6, -4.4),(2, -9.6, 3),(3, -1.3, -4),(4, 5.3, 9.7),(5, 4.4, 0.037),(6, -8.6, -7.8),(7, 5.1, 9.3),(8, 7.9, -3.6),(9, -8.2, 0.62),(10, -3, 7.3);

SELECT covarPop(x_value, y_value)
FROM series
        )",
        R"(
┌─covarPop(x_value, y_value)─┐
│                   6.485648 │
└────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::Category covarPop_category = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation::IntroducedIn covarPop_introduced_in = {1, 1};
    FunctionDocumentation covarPop_documentation = {covarPop_description, covarPop_syntax, covarPop_arguments, covarPop_parameters, covarPop_returned_value, covarPop_examples, covarPop_introduced_in, covarPop_category};
    factory.registerFunction("covarPop", {createAggregateFunctionStatisticsBinary<AggregateFunctionCovar, StatisticsFunctionKind::covarPop>, AggregateFunctionProperties{}, covarPop_documentation });

    /// Synonyms for compatibility.
    factory.registerAlias("COVAR_SAMP", "covarSamp", AggregateFunctionFactory::Case::Insensitive);
    factory.registerAlias("COVAR_POP", "covarPop", AggregateFunctionFactory::Case::Insensitive);
}

}
