#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsVisitParam.h>
#include <Functions/FunctionsStringSearch.h>


namespace DB
{

struct ExtractBool
{
    using ResultType = UInt8;

    static UInt8 extract(const UInt8 * begin, const UInt8 * end)
    {
        return begin + 4 <= end && 0 == strncmp(reinterpret_cast<const char *>(begin), "true", 4);
    }
};

struct NameSimpleJSONExtractBool { static constexpr auto name = "simpleJSONExtractBool"; };
using FunctionSimpleJSONExtractBool = FunctionsStringSearch<ExtractParamImpl<NameSimpleJSONExtractBool, ExtractBool>>;

REGISTER_FUNCTION(VisitParamExtractBool)
{
    factory.registerFunction<FunctionSimpleJSONExtractBool>(FunctionDocumentation{
        .description = "Parses a true/false value from the value of the field named field_name. The result is UInt8.",
        .syntax = "simpleJSONExtractBool(json, field_name)",
        .arguments
        = {{"json", "The JSON in which the field is searched for.", {"String"}},
           {"field_name", "The name of the field to search for.", {"const String"}}},
        .returned_value
        = {R"(It returns 1 if the value of the field is true, 0 otherwise. This means this function will return 0 including (and not only) in the following cases:
 - If the field doesn't exists.
 - If the field contains true as a string, e.g.: {"field":"true"}.
 - If the field contains 1 as a numerical value.)"},
        .examples
        = {{.name = "simple",
            .query = R"(CREATE TABLE jsons
(
    json String
)
ENGINE = Memory;

INSERT INTO jsons VALUES ('{"foo":false,"bar":true}');
INSERT INTO jsons VALUES ('{"foo":"true","qux":1}');

SELECT simpleJSONExtractBool(json, 'bar') FROM jsons ORDER BY json;
SELECT simpleJSONExtractBool(json, 'foo') FROM jsons ORDER BY json;)",
            .result = R"(0
1
0
0)"}},
        .category = FunctionDocumentation::Category::JSON});
    factory.registerAlias("visitParamExtractBool", "simpleJSONExtractBool");
}

}
