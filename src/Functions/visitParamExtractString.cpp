#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsVisitParam.h>
#include <Functions/FunctionsStringSearchToString.h>


namespace DB
{

struct ExtractString
{
    static void extract(const UInt8 * pos, const UInt8 * end, ColumnString::Chars & res_data)
    {
        size_t old_size = res_data.size();
        ReadBufferFromMemory in(pos, end - pos);
        if (!tryReadJSONStringInto(res_data, in, default_json_settings))
            res_data.resize(old_size);
    }

    static const FormatSettings::JSON constexpr default_json_settings;
};

struct NameSimpleJSONExtractString { static constexpr auto name = "simpleJSONExtractString"; };
using FunctionSimpleJSONExtractString = FunctionsStringSearchToString<ExtractParamToStringImpl<ExtractString>, NameSimpleJSONExtractString>;

REGISTER_FUNCTION(VisitParamExtractString)
{
    FunctionDocumentation::Description description = R"(
Parses `String` in double quotes from the value of the field named `field_name`.

**Implementation details**

There is currently no support for code points in the format `\uXXXX\uYYYY` that are not from the basic multilingual plane (they are converted to CESU-8 instead of UTF-8).
)";
    FunctionDocumentation::Syntax syntax = "simpleJSONExtractString(json, field_name)";
    FunctionDocumentation::Arguments arguments = {
        {"json", "The JSON in which the field is searched for.", {"String"}},
        {"field_name", "The name of the field to search for.", {"const String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the unescaped value of a field as a string, including separators. An empty string is returned if the field doesn't contain a double quoted string, if unescaping fails or if the field doesn't exist", {"String"}};
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

INSERT INTO jsons VALUES ('{"foo":"\\n\\u0000"}');
INSERT INTO jsons VALUES ('{"foo":"\\u263"}');
INSERT INTO jsons VALUES ('{"foo":"\\u263a"}');
INSERT INTO jsons VALUES ('{"foo":"hello}');

SELECT simpleJSONExtractString(json, 'foo') FROM jsons ORDER BY json;
        )",
        R"(
\n\0

☺

        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {21, 4};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::JSON;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, example, introduced_in, category};

    factory.registerFunction<FunctionSimpleJSONExtractString>(documentation);
    factory.registerAlias("visitParamExtractString", "simpleJSONExtractString");
}

}
