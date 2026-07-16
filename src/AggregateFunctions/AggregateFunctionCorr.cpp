#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionStatisticsSimple.h>


namespace DB
{

template <typename T1, typename T2> using AggregateFunctionCorr = AggregateFunctionVarianceSimple<StatFuncTwoArg<T1, T2, CorrMoments>>;

void registerAggregateFunctionsStatisticsCorr(AggregateFunctionFactory & factory)
{
    FunctionDocumentation::Description description = R"(
Calculates the [Pearson correlation coefficient](https://en.wikipedia.org/wiki/Pearson_correlation_coefficient):

$$
\frac{\Sigma{(x - \bar{x})(y - \bar{y})}}{\sqrt{\Sigma{(x - \bar{x})^2} * \Sigma{(y - \bar{y})^2}}}
$$

<br/>

:::note
This function uses a numerically unstable algorithm. If you need [numerical stability](https://en.wikipedia.org/wiki/Numerical_stability) in calculations, use the [`corrStable`](../reference/corrStable.md) function. It is slower but provides a more accurate result.
:::
    )";
    FunctionDocumentation::Syntax syntax = "corr(x, y)";
    FunctionDocumentation::Arguments arguments = {
        {"x", "First variable.", {"(U)Int*", "Float*"}},
        {"y", "Second variable.", {"(U)Int*", "Float*"}}
    };
    FunctionDocumentation::Parameters parameters = {};
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the Pearson correlation coefficient.", {"Float64"}};
    FunctionDocumentation::Examples examples = {
    {
        "Basic correlation calculation",
        R"(
DROP TABLE IF EXISTS series;
CREATE TABLE series
(
    i UInt32,
    x_value Float64,
    y_value Float64
)
ENGINE = Memory;
INSERT INTO series(i, x_value, y_value) VALUES (1, 5.6, -4.4),(2, -9.6, 3),(3, -1.3, -4),(4, 5.3, 9.7),(5, 4.4, 0.037),(6, -8.6, -7.8),(7, 5.1, 9.3),(8, 7.9, -3.6),(9, -8.2, 0.62),(10, -3, 7.3);

SELECT corr(x_value, y_value)
FROM series
        )",
        R"(
┌─corr(x_value, y_value)─┐
│     0.1730265755453256 │
└────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::Category category = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation documentation = {description, syntax, arguments, parameters, returned_value, examples, introduced_in, category};
    factory.registerFunction("corr", {createAggregateFunctionStatisticsBinary<AggregateFunctionCorr, StatisticsFunctionKind::corr>, documentation }, AggregateFunctionFactory::Case::Insensitive);
}

}
