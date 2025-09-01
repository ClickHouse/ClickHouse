#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsVisitParam.h>
#include <Functions/FunctionsStringSearchToString.h>


namespace DB
{

struct ExtractRaw
{
    using ExpectChars = PODArrayWithStackMemory<char, 64>;

    static void extract(const UInt8 * pos, const UInt8 * end, ColumnString::Chars & res_data)
    {
        ExpectChars expects_end;
        UInt8 current_expect_end = 0;

        for (const auto * extract_begin = pos; pos != end; ++pos)
        {
            if (current_expect_end && *pos == current_expect_end)
            {
                expects_end.pop_back();
                current_expect_end = expects_end.empty() ? 0 : expects_end.back();
            }
            else if (current_expect_end == '"')
            {
                /// skip backslash
                if (*pos == '\\' && pos + 1 < end && pos[1] == '"')
                    ++pos;
            }
            else
            {
                switch (*pos)
                {
                    case '[':
                        current_expect_end = ']';
                        expects_end.push_back(current_expect_end);
                        break;
                    case '{':
                        current_expect_end = '}';
                        expects_end.push_back(current_expect_end);
                        break;
                    case '"' :
                        current_expect_end = '"';
                        expects_end.push_back(current_expect_end);
                        break;
                    default:
                        if (!current_expect_end && (*pos == ',' || *pos == '}'))
                        {
                            res_data.insert(extract_begin, pos);
                            return;
                        }
                }
            }
        }
    }
};

struct NameSimpleJSONExtractRaw    { static constexpr auto name = "simpleJSONExtractRaw"; };
using FunctionSimpleJSONExtractRaw = FunctionsStringSearchToString<ExtractParamToStringImpl<ExtractRaw>, NameSimpleJSONExtractRaw>;

REGISTER_FUNCTION(VisitParamExtractRaw)
{
    factory.registerFunction<FunctionSimpleJSONExtractRaw>(FunctionDocumentation{
        .description = "Returns the value of the field named field_name as a String, including separators.",
        .syntax = "simpleJSONExtractRaw(json, field_name)",
        .arguments
        = {{"json", "The JSON in which the field is searched for.", {"String"}},
           {"field_name", "The name of the field to search for.", {"const String"}}},
        .returned_value = {"The value of the field as a String including separators if the field exists, or an empty String otherwise.", {"String"}},
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
INSERT INTO jsons VALUES ('{"foo":{"def":[1,2,3]}}');
INSERT INTO jsons VALUES ('{"baz":2}');

SELECT simpleJSONExtractRaw(json, 'foo') FROM jsons ORDER BY json;)",
            .result = R"(
"-4e3"
-3.4
5
{"def":[1,2,3]})"}},
        .category = FunctionDocumentation::Category::JSON});
    factory.registerAlias("visitParamExtractRaw", "simpleJSONExtractRaw");
}

}
