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
    FunctionDocumentation::Description description = R"(
Checks whether there is a field named `field_name`.
)";
    FunctionDocumentation::Syntax syntax = "simpleJSONHas(json, field_name)";
    FunctionDocumentation::Arguments arguments = {
        {"json", "The JSON in which the field is searched for.", {"String"}},
        {"field_name", "The name of the field to search for.", {"const String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns `1` if the field exists, `0` otherwise",{"UInt8"}};
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

INSERT INTO jsons VALUES ('{"foo":"true","qux":1}');

SELECT simpleJSONHas(json, 'foo') FROM jsons;
SELECT simpleJSONHas(json, 'bar') FROM jsons;
        )",
        R"(
1
0
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {21, 4};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::JSON;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, example, introduced_in, category};

    factory.registerFunction<FunctionSimpleJSONHas>(documentation);
    factory.registerAlias("visitParamHas", "simpleJSONHas");
}

}
