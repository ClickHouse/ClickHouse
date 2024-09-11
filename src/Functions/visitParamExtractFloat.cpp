#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsVisitParam.h>
#include <Functions/FunctionsStringSearch.h>


namespace DB
{

struct NameSimpleJSONExtractFloat { static constexpr auto name = "simpleJSONExtractFloat"; };
using FunctionSimpleJSONExtractFloat = FunctionsStringSearch<ExtractParamImpl<NameSimpleJSONExtractFloat, ExtractNumericType<Float64>>>;

REGISTER_FUNCTION(VisitParamExtractFloat)
{
    factory.registerFunction<FunctionSimpleJSONExtractFloat>(FunctionDocumentation{
        .description
        = "Parses Float64 from the value of the field named field_name. If this is a string field, it tries to parse a number from the "
          "beginning of the string. If the field does not exist, or it exists but does not contain a number, it returns 0.",
        .syntax = "simpleJSONExtractFloat(json, field_name)",
        .arguments
        = {{"json", "The JSON in which the field is searched for. String."},
           {"field_name", "The name of the field to search for. String literal."}},
        .returned_value = "It returns the number parsed from the field if the field exists and contains a number, 0 otherwise.",
        .examples
        = {{.name = "simple",
            .query = R"(CREATE TABLE jsons
(
    json String
)
ENGINE = Memory;

INSERT INTO jsons VALUES ('{"foo":"-4e3"}');
INSERT INTO jsons VALUES ('{"foo":-3.4}');
INSERT INTO jsons VALUES ('{"foo":5}');
INSERT INTO jsons VALUES ('{"foo":"not1number"}');
INSERT INTO jsons VALUES ('{"baz":2}');

SELECT simpleJSONExtractFloat(json, 'foo') FROM jsons ORDER BY json;)",
            .result = R"(0
-4000
0
-3.4
5)"}},
        .categories{"JSON"}});
    factory.registerAlias("visitParamExtractFloat", "simpleJSONExtractFloat");
}

}
