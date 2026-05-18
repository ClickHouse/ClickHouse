#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsVisitParam.h>
#include <Functions/FunctionsStringSearch.h>


namespace DB
{

struct NameSimpleJSONExtractInt { static constexpr auto name = "simpleJSONExtractInt"; };
using FunctionSimpleJSONExtractInt = FunctionsStringSearch<ExtractParamImpl<NameSimpleJSONExtractInt, ExtractNumericType<Int64>>>;

REGISTER_FUNCTION(VisitParamExtractInt)
{
    FunctionDocumentation::Description description = R"(
Parses `Int64` from the value of the field named `field_name`.
If `field_name` is a string field, it tries to parse a number from the beginning of the string.
If the field does not exist, or it exists but does not contain a number, it returns `0`.
)";
    FunctionDocumentation::Syntax syntax = "simpleJSONExtractInt(json, field_name)";
    FunctionDocumentation::Arguments arguments = {
        {"json", "The JSON in which the field is searched for.", {"String"}},
        {"field_name", "The name of the field to search for.", {"const String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the number parsed from the field if the field exists and contains a number, `0` otherwise", {"Int64"}};
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

SELECT simpleJSONExtractInt(json, 'foo') FROM jsons ORDER BY json;
        )",
        R"(
0
-4
0
-3
5
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {21, 4};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::JSON;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, example, introduced_in, category};

    factory.registerFunction<FunctionSimpleJSONExtractInt>(documentation);
    factory.registerAlias("visitParamExtractInt", "simpleJSONExtractInt");
}

}
