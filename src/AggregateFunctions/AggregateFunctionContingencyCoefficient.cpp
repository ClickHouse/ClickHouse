#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/CrossTab.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <memory>
#include <cmath>


namespace DB
{

namespace
{

struct ContingencyData : CrossTabData
{
    static const char * getName()
    {
        return "contingency";
    }

    /**
    * Based on https://en.wikipedia.org/wiki/Contingency_table#Cram%C3%A9r's_V_and_the_contingency_coefficient_C
    *
    * Pearson's contingency coefficient is defined as:
    * C = sqrt(χ² / (χ² + n)), where χ² is the chi-squared statistic and n is the total number of observations.
    *
    * We have,
    * phi_squared, φ² = χ² / n
    * => χ² = n · φ².
    *
    * Substituting χ² into the formula for C,
    * C = sqrt((n · φ²) / (n · φ² + n))
    *   = sqrt(φ² / (φ² + 1)).
    **/

    Float64 getResult() const
    {
        if (count < 2)
            return std::numeric_limits<Float64>::quiet_NaN();

        Float64 phi_sq = getPhiSquared();
        return std::sqrt(phi_sq / (phi_sq + 1.0));
    }
};

}

void registerAggregateFunctionContingency(AggregateFunctionFactory & factory)
{
    FunctionDocumentation::Description description = R"(
The `contingency` function calculates the [contingency coefficient](https://en.wikipedia.org/wiki/Contingency_table#Cram%C3%A9r's_V_and_the_contingency_coefficient_C), a value that measures the association between two columns in a table.
The computation is similar to the [`cramersV`](/sql-reference/aggregate-functions/reference/cramersv) function but with a different denominator in the square root.
    )";
    FunctionDocumentation::Syntax syntax = "contingency(column1, column2)";
    FunctionDocumentation::Arguments arguments = {
        {"column1", "First column to compare.", {"Any"}},
        {"column2", "Second column to compare.", {"Any"}}
    };
    FunctionDocumentation::Parameters docs_parameters = {};
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a value between 0 and 1. The larger the result, the closer the association of the two columns.", {"Float64"}};
    FunctionDocumentation::Examples examples = {
    {
        "Comparison with cramersV",
        R"(
SELECT
    cramersV(a, b),
    contingency(a, b)
FROM
(
    SELECT
        number % 10 AS a,
        number % 4 AS b
    FROM
        numbers(150)
)
        )",
        R"(
┌──────cramersV(a, b)─┬───contingency(a, b)─┐
│ 0.41171788506213564 │ 0.05812725261759165 │
└─────────────────────┴─────────────────────┘
        )"
    }
    };
    FunctionDocumentation::Category category = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation::IntroducedIn introduced_in = {22, 1};
    FunctionDocumentation documentation = {description, syntax, arguments, docs_parameters, returned_value, examples, introduced_in, category};
    factory.registerFunction(ContingencyData::getName(),
    {
        [](const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
        {
            assertBinary(name, argument_types);
            assertNoParameters(name, parameters);
            return std::make_shared<AggregateFunctionCrossTab<ContingencyData>>(argument_types);
        },
        documentation
    });
}

}
