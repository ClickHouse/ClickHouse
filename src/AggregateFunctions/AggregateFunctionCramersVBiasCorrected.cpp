#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/CrossTab.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <memory>
#include <cmath>


namespace DB
{

namespace
{

struct CramersVBiasCorrectedData : CrossTabAggregateData
{
    static const char * getName()
    {
        return "cramersVBiasCorrected";
    }

    Float64 getResult() const
    {
        if (count < 2)
            return std::numeric_limits<Float64>::quiet_NaN();

        Float64 phi = getPhiSquared();

        Float64 a_size = static_cast<Float64>(count_a.size());
        Float64 b_size = static_cast<Float64>(count_b.size());

        Float64 a_size_adjusted = a_size - 1;
        Float64 b_size_adjusted = b_size - 1;
        Float64 count_adjusted = static_cast<Float64>(count - 1);

        Float64 res = std::max(0.0, phi - a_size_adjusted * b_size_adjusted / count_adjusted);
        Float64 correction_a = a_size - a_size_adjusted * a_size_adjusted / count_adjusted;
        Float64 correction_b = b_size - b_size_adjusted * b_size_adjusted / count_adjusted;

        const Float64 denom = std::min(correction_a, correction_b) - 1.0;
        if (denom <= 0.0)
            return std::numeric_limits<Float64>::quiet_NaN();

        res /= denom;
        return sqrt(res);
    }
};

struct CramersVBiasCorrectedWindowData : CrossTabPhiSquaredWindowData
{
    static const char * getName()
    {
        return CramersVBiasCorrectedData::getName();
    }

    Float64 getResult() const
    {
        if (count < 2)
            return std::numeric_limits<Float64>::quiet_NaN();

        Float64 phi = getPhiSquared();

        Float64 a_size = static_cast<Float64>(a_marginal_count.size());
        Float64 b_size = static_cast<Float64>(b_marginal_count.size());

        Float64 a_size_adjusted = a_size - 1;
        Float64 b_size_adjusted = b_size - 1;
        Float64 count_adjusted = static_cast<Float64>(count - 1);

        Float64 res = std::max(0.0, phi - a_size_adjusted * b_size_adjusted / count_adjusted);
        Float64 correction_a = a_size - a_size_adjusted * a_size_adjusted / count_adjusted;
        Float64 correction_b = b_size - b_size_adjusted * b_size_adjusted / count_adjusted;

        const Float64 denom = std::min(correction_a, correction_b) - 1.0;
        if (denom <= 0.0)
            return std::numeric_limits<Float64>::quiet_NaN();

        res /= denom;
        return sqrt(res);
    }
};

}

void registerAggregateFunctionCramersVBiasCorrected(AggregateFunctionFactory & factory)
{
    FunctionDocumentation::Description description = R"(
Cramer's V is a measure of association between two columns in a table.
The result of the [`cramersV` function](./cramersv.md) ranges from 0 (corresponding to no association between the variables) to 1 and can reach 1 only when each value is completely determined by the other.
The function can be heavily biased, so this version of Cramer's V uses the [bias correction](https://en.wikipedia.org/wiki/Cram%C3%A9r%27s_V#Bias_correction).
    )";
    FunctionDocumentation::Syntax syntax = "cramersVBiasCorrected(column1, column2)";
    FunctionDocumentation::Arguments arguments = {
        {"column1", "First column to be compared.", {"Any"}},
        {"column2", "Second column to be compared.", {"Any"}}
    };
    FunctionDocumentation::Parameters docs_parameters = {};
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a value between 0 (corresponding to no association between the columns' values) to 1 (complete association).", {"Float64"}};
    FunctionDocumentation::Examples examples = {
    {
        "Comparison with regular cramersV",
        R"(
SELECT
    cramersV(a, b),
    cramersVBiasCorrected(a, b)
FROM
    (
        SELECT
            number % 10 AS a,
            number % 4 AS b
        FROM
            numbers(150)
    );
        )",
        R"(
┌─────cramersV(a, b)─┬─cramersVBiasCorrected(a, b)─┐
│ 0.5798088336225178 │          0.5305112825189074 │
└────────────────────┴─────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::Category category = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation::IntroducedIn introduced_in = {22, 1};
    FunctionDocumentation documentation = {description, syntax, arguments, docs_parameters, returned_value, examples, introduced_in, category};
    factory.registerFunction(CramersVBiasCorrectedData::getName(),
    {
        [](const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
        {
            assertBinary(name, argument_types);
            assertNoParameters(name, parameters);
            return std::make_shared<AggregateFunctionCrossTab<CramersVBiasCorrectedData>>(argument_types);
        },
        {},
        documentation,
        [](const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
        {
            assertBinary(name, argument_types);
            assertNoParameters(name, parameters);
            return std::make_shared<AggregateFunctionCrossTab<CramersVBiasCorrectedWindowData>>(argument_types);
        }
    });
}

}
