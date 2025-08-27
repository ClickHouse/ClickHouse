#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsVisitParam.h>
#include <Functions/FunctionsStringSearch.h>


namespace DB
{

struct HasParam
{
    using ResultType = UInt8;

    static UInt8 extract(const UInt8 *, const UInt8 *)
    {
        return true;
    }
};

struct NameSimpleJSONHas { static constexpr auto name = "simpleJSONHas"; };
using FunctionSimpleJSONHas = FunctionsStringSearch<ExtractParamImpl<NameSimpleJSONHas, HasParam>>;

REGISTER_FUNCTION(VisitParamHas)
{
    factory.registerFunction<FunctionSimpleJSONHas>(FunctionDocumentation{
        .description = "Checks whether there is a field named field_name.  The result is UInt8.",
        .syntax = "simpleJSONHas(json, field_name)",
        .arguments
        = {{"json", "The JSON in which the field is searched for.", {"String"}},
           {"field_name", "The name of the field to search for.", {"const String"}}},
        .returned_value = {"It returns 1 if the field exists, 0 otherwise."},
        .examples
        = {{.name = "simple",
            .query = R"(CREATE TABLE jsons
(
    json String
)
ENGINE = Memory;

INSERT INTO jsons VALUES ('{"foo":"true","qux":1}');

SELECT simpleJSONHas(json, 'foo') FROM jsons;
SELECT simpleJSONHas(json, 'bar') FROM jsons;)",
            .result = R"(1
0)"}},
        .category = FunctionDocumentation::Category::JSON});
    factory.registerAlias("visitParamHas", "simpleJSONHas");
}

}
