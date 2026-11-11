#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/CrossTab.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <memory>
#include <cmath>


namespace DB
{

namespace
{

struct TheilsUData : CrossTabData
{
    static const char * getName()
    {
        return "theilsU";
    }

    /// Based on https://en.wikipedia.org/wiki/Uncertainty_coefficient.
    Float64 getResult() const
    {
        if (count < 2)
            return std::numeric_limits<Float64>::quiet_NaN();

        Float64 h_a = 0.0;
        for (const auto & [key, value] : count_a)
        {
            Float64 value_float = static_cast<Float64>(value);
            Float64 prob_a = value_float / static_cast<Float64>(count);
            h_a += prob_a * log(prob_a);
        }

        if (h_a == 0.0)
            return 0.0;

        Float64 dep = 0.0;
        for (const auto & [key, value] : count_ab)
        {
            Float64 value_ab = static_cast<Float64>(value);
            Float64 value_b = static_cast<Float64>(count_b.at(key.items[UInt128::_impl::little(1)]));
            Float64 prob_ab = value_ab / static_cast<Float64>(count);
            Float64 prob_a_given_b = value_ab / value_b;
            dep += prob_ab * log(prob_a_given_b);
        }

        Float64 coef = (h_a - dep) / h_a;
        return coef;
    }
};

}

void registerAggregateFunctionTheilsU(AggregateFunctionFactory & factory)
{
    FunctionDocumentation::Description description = R"(
The `theilsU` function calculates the [Theil's U uncertainty coefficient](https://en.wikipedia.org/wiki/Contingency_table#Uncertainty_coefficient), a value that measures the association between two columns in a table.
Its values range from −1.0 (100% negative association, or perfect inversion) to +1.0 (100% positive association, or perfect agreement).
A value of 0.0 indicates the absence of association.
    )";
    FunctionDocumentation::Syntax syntax = R"(
theilsU(column1, column2)
    )";
    FunctionDocumentation::Arguments arguments = {
        {"column1", "First column to be compared.", {"Any"}},
        {"column2", "Second column to be compared.", {"Any"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {
        "Returns a value between -1 and 1.", {"Float64"}
    };
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        R"(
SELECT theilsU(a, b)
FROM (
    SELECT
        number % 10 AS a,
        number % 4 AS b
    FROM
        numbers(150)
);
        )",
        R"(
┌────────theilsU(a, b)─┐
│ -0.30195720557678846 │
└──────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {22, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction(TheilsUData::getName(), {
        [](const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
        {
            assertBinary(name, argument_types);
            assertNoParameters(name, parameters);
            return std::make_shared<AggregateFunctionCrossTab<TheilsUData>>(argument_types);
        }, documentation});
}

}
