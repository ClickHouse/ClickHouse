#include <Functions/FunctionFactory.h>

#include <Functions/UniqTheta/FunctionsUniqTheta.h>

#if USE_DATASKETCHES

namespace DB
{

REGISTER_FUNCTION(UniqTheta)
{
    /// uniqThetaUnion documentation
    FunctionDocumentation::Description description_uniqThetaUnion = R"(
Two uniqThetaSketch objects to do union calculation(set operation ∪), the result is a new uniqThetaSketch.
    )";
    FunctionDocumentation::Syntax syntax_uniqThetaUnion = "uniqThetaUnion(uniqThetaSketch,uniqThetaSketch)";
    FunctionDocumentation::Arguments arguments_uniqThetaUnion = {
        {"uniqThetaSketch", "uniqThetaSketch object.", {"Tuple", "Array", "Date", "DateTime", "String", "(U)Int*", "Float*", "Decimal"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_uniqThetaUnion = {"Returns a new uniqThetaSketch containing the union result.", {"UInt64"}};
    FunctionDocumentation::Examples examples_uniqThetaUnion = {
    {
        "Usage example",
        R"(
SELECT finalizeAggregation(uniqThetaUnion(a, b)) AS a_union_b, finalizeAggregation(a) AS a_cardinality, finalizeAggregation(b) AS b_cardinality
FROM
(SELECT arrayReduce('uniqThetaState', [1, 2]) AS a, arrayReduce('uniqThetaState', [2, 3, 4]) AS b);
        )",
        R"(
┌─a_union_b─┬─a_cardinality─┬─b_cardinality─┐
│         4 │             2 │             3 │
└───────────┴───────────────┴───────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_uniqThetaUnion = {22, 9};
    FunctionDocumentation::Category category_uniqThetaUnion = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation_uniqThetaUnion = {description_uniqThetaUnion, syntax_uniqThetaUnion, arguments_uniqThetaUnion, returned_value_uniqThetaUnion, examples_uniqThetaUnion, introduced_in_uniqThetaUnion, category_uniqThetaUnion};

    /// uniqThetaIntersect documentation
    FunctionDocumentation::Description description_uniqThetaIntersect = R"(
Two uniqThetaSketch objects to do intersect calculation(set operation ∩), the result is a new uniqThetaSketch.
    )";
    FunctionDocumentation::Syntax syntax_uniqThetaIntersect = "uniqThetaIntersect(uniqThetaSketch,uniqThetaSketch)";
    FunctionDocumentation::Arguments arguments_uniqThetaIntersect = {
        {"uniqThetaSketch", "uniqThetaSketch object.", {"Tuple", "Array", "Date", "DateTime", "String", "(U)Int*", "Float*", "Decimal"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_uniqThetaIntersect = {"A new uniqThetaSketch containing the intersect result.", {"UInt64"}};
    FunctionDocumentation::Examples examples_uniqThetaIntersect = {
    {
        "Usage example",
        R"(
SELECT finalizeAggregation(uniqThetaIntersect(a, b)) AS a_intersect_b, finalizeAggregation(a) AS a_cardinality, finalizeAggregation(b) AS b_cardinality
FROM
(SELECT arrayReduce('uniqThetaState', [1, 2]) AS a, arrayReduce('uniqThetaState', [2, 3, 4]) AS b);
        )",
        R"(
┌─a_intersect_b─┬─a_cardinality─┬─b_cardinality─┐
│             1 │             2 │             3 │
└───────────────┴───────────────┴───────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_uniqThetaIntersect = {22, 9};
    FunctionDocumentation::Category category_uniqThetaIntersect = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation_uniqThetaIntersect = {description_uniqThetaIntersect, syntax_uniqThetaIntersect, arguments_uniqThetaIntersect, returned_value_uniqThetaIntersect, examples_uniqThetaIntersect, introduced_in_uniqThetaIntersect, category_uniqThetaIntersect};

    /// uniqThetaNot documentation
    FunctionDocumentation::Description description_uniqThetaNot = R"(
Two uniqThetaSketch objects to do a_not_b calculation(set operation ×), the result is a new uniqThetaSketch.
    )";
    FunctionDocumentation::Syntax syntax_uniqThetaNot = "uniqThetaNot(uniqThetaSketch,uniqThetaSketch)";
    FunctionDocumentation::Arguments arguments_uniqThetaNot = {
        {"uniqThetaSketch", "uniqThetaSketch object.", {"Tuple", "Array", "Date", "DateTime", "String", "(U)Int*", "Float*", "Decimal"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_uniqThetaNot = {"Returns a new uniqThetaSketch containing the a_not_b result.", {"UInt64"}};
    FunctionDocumentation::Examples examples_uniqThetaNot = {
    {
        "Usage example",
        R"(
SELECT finalizeAggregation(uniqThetaNot(a, b)) AS a_not_b, finalizeAggregation(a) AS a_cardinality, finalizeAggregation(b) AS b_cardinality
FROM
(SELECT arrayReduce('uniqThetaState', [2, 3, 4]) AS a, arrayReduce('uniqThetaState', [1, 2]) AS b);
        )",
        R"(
┌─a_not_b─┬─a_cardinality─┬─b_cardinality─┐
│       2 │             3 │             2 │
└─────────┴───────────────┴───────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_uniqThetaNot = {22, 9};
    FunctionDocumentation::Category category_uniqThetaNot = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation_uniqThetaNot = {description_uniqThetaNot, syntax_uniqThetaNot, arguments_uniqThetaNot, returned_value_uniqThetaNot, examples_uniqThetaNot, introduced_in_uniqThetaNot, category_uniqThetaNot};

    factory.registerFunction<FunctionUniqThetaUnion>(documentation_uniqThetaUnion);
    factory.registerFunction<FunctionUniqThetaIntersect>(documentation_uniqThetaIntersect);
    factory.registerFunction<FunctionUniqThetaNot>(documentation_uniqThetaNot);
}

}

#endif
