#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/CrossTab.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <memory>
#include <cmath>


namespace DB
{

namespace
{

struct CramersVData : CrossTabData
{
    static const char * getName()
    {
        return "cramersV";
    }

    Float64 getResult() const
    {
        if (count < 2)
            return std::numeric_limits<Float64>::quiet_NaN();

        UInt64 q = std::min(count_a.size(), count_b.size());
        return sqrt(getPhiSquared() / static_cast<Float64>(q - 1));
    }
};

}

void registerAggregateFunctionCramersV(AggregateFunctionFactory & factory)
{
    FunctionDocumentation::Description description = R"(
[Cramer's V](https://en.wikipedia.org/wiki/Cram%C3%A9r%27s_V) (sometimes referred to as Cramer's phi) is a measure of association between two columns in a table.
The result of the `cramersV` function ranges from 0 (corresponding to no association between the variables) to 1 and can reach 1 only when each value is completely determined by the other.
It may be viewed as the association between two variables as a percentage of their maximum possible variation.

:::note
For a bias corrected version of Cramer's V see: [cramersVBiasCorrected](/sql-reference/aggregate-functions/reference/cramersvbiascorrected)
:::
    )";
    FunctionDocumentation::Syntax syntax = "cramersV(column1, column2)";
    FunctionDocumentation::Arguments arguments = {
        {"column1", "First column to be compared.", {"(U)Int*", "Float*", "Decimal"}},
        {"column2", "Second column to be compared.", {"(U)Int*", "Float*", "Decimal"}}
    };
    FunctionDocumentation::Parameters docs_parameters = {};
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a value between 0 (corresponding to no association between the columns' values) to 1 (complete association).", {"Float64"}};
    FunctionDocumentation::Examples examples = {
    {
        "No association between columns",
        R"(
SELECT
    cramersV(a, b)
FROM
    (
        SELECT
            number % 3 AS a,
            number % 5 AS b
        FROM
            numbers(150)
    )
        )",
        R"(
┌─cramersV(a, b)─┐
│              0 │
└────────────────┘
        )"
    },
    {
        "High association between columns",
        R"(
SELECT
    cramersV(a, b)
FROM
    (
        SELECT
            number % 10 AS a,
            number % 5 AS b
        FROM
            numbers(150)
    )
        )",
        R"(
┌─────cramersV(a, b)─┐
│ 0.8944271909999159 │
└────────────────────┘
        )"
    }
    };
    FunctionDocumentation::Category category = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation::IntroducedIn introduced_in = {22, 1};
    FunctionDocumentation documentation = {description, syntax, arguments, docs_parameters, returned_value, examples, introduced_in, category};
    factory.registerFunction(CramersVData::getName(),
    {
        [](const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
        {
            assertBinary(name, argument_types);
            assertNoParameters(name, parameters);
            return std::make_shared<AggregateFunctionCrossTab<CramersVData>>(argument_types);
        },
        documentation
    });
}

}
