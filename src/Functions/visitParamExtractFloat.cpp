#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsVisitParam.h>
#include <Functions/FunctionsStringSearch.h>


namespace DB
{

struct NameSimpleJSONExtractFloat { static constexpr auto name = "simpleJSONExtractFloat"; };
using FunctionSimpleJSONExtractFloat = FunctionsStringSearch<ExtractParamImpl<NameSimpleJSONExtractFloat, ExtractNumericType<Float64>>>;

REGISTER_FUNCTION(VisitParamExtractFloat)
{
    FunctionDocumentation::Description description = R"(
Parses `Float64` from the value of the field named `field_name`.
If `field_name` is a string field, it tries to parse a number from the beginning of the string.
If the field does not exist, or it exists but does not contain a number, it returns `0`.
)";
    FunctionDocumentation::Syntax syntax = "simpleJSONExtractFloat(json, field_name)";
    FunctionDocumentation::Arguments arguments = {
        {"json", "The JSON in which the field is searched for.", {"String"}},
        {"field_name", "The name of the field to search for.", {"const String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the number parsed from the field if the field exists and contains a number, otherwise `0`.", {"Float64"}};
    FunctionDocumentation::Examples example = {
    {
        "Usage example",
        R"(
CREATE TABLE jsons
(
    `json` String
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO jsons VALUES ('{"foo":"-4e3"}');
INSERT INTO jsons VALUES ('{"foo":-3.4}');
INSERT INTO jsons VALUES ('{"foo":5}');
INSERT INTO jsons VALUES ('{"foo":"not1number"}');
INSERT INTO jsons VALUES ('{"baz":2}');

SELECT simpleJSONExtractFloat(json, 'foo') FROM jsons ORDER BY json;
        )",
        R"(
0
-4000
0
-3.4
5
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {21, 4};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::JSON;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, example, introduced_in, category};

    factory.registerFunction<FunctionSimpleJSONExtractFloat>(documentation);
    factory.registerAlias("visitParamExtractFloat", "simpleJSONExtractFloat");
}

}
