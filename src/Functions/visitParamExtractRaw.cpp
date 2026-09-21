#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsVisitParam.h>
#include <Functions/FunctionsStringSearchToString.h>
#include <base/find_symbols.h>


namespace DB
{

struct ExtractRaw
{
    using ExpectChars = PODArrayWithStackMemory<char, 64>;
    using Scratch = ExpectChars;

    static void extract(const UInt8 * pos, const UInt8 * end, ColumnString::Chars & res_data, Scratch & expects_end)
    {
        expects_end.clear();
        UInt8 current_expect_end = 0;
        const auto * const extract_begin = pos;

        while (pos != end)
        {
            /// Most bytes of a typical value sit inside a string, where only `"` and `\` matter, so jump
            /// to the next one instead of walking the state machine over every character.
            if (current_expect_end == '"')
            {
                pos = reinterpret_cast<const UInt8 *>(find_first_symbols<'"', '\\'>(
                    reinterpret_cast<const char *>(pos), reinterpret_cast<const char *>(end)));
                if (pos == end)
                    return;

                if (*pos == '"')
                {
                    expects_end.pop_back();
                    current_expect_end = expects_end.empty() ? 0 : expects_end.back();
                }
                else if (pos + 1 < end && pos[1] == '"')
                {
                    /// A backslash only escapes a `"`, as in the character by character scan.
                    ++pos;
                }

                ++pos;
                continue;
            }

            if (current_expect_end && *pos == current_expect_end)
            {
                expects_end.pop_back();
                current_expect_end = expects_end.empty() ? 0 : expects_end.back();
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

            ++pos;
        }
    }
};

struct NameSimpleJSONExtractRaw    { static constexpr auto name = "simpleJSONExtractRaw"; };
using FunctionSimpleJSONExtractRaw = FunctionsStringSearchToString<ExtractParamToStringImpl<ExtractRaw>, NameSimpleJSONExtractRaw>;

REGISTER_FUNCTION(VisitParamExtractRaw)
{
    FunctionDocumentation::Description description = R"(
Returns the value of the field named `field_name` as a `String`, including separators.
)";
    FunctionDocumentation::Syntax syntax = "simpleJSONExtractRaw(json, field_name)";
    FunctionDocumentation::Arguments arguments = {
        {"json", "The JSON in which the field is searched for.", {"String"}},
        {"field_name", "The name of the field to search for.", {"const String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {
        "Returns the value of the field as a string, including separators if the field exists, or an empty string otherwise",
        {"String"}
    };
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
INSERT INTO jsons VALUES ('{"foo":{"def":[1,2,3]}}');
INSERT INTO jsons VALUES ('{"baz":2}');

SELECT simpleJSONExtractRaw(json, 'foo') FROM jsons ORDER BY json;
        )",
        R"(

"-4e3"
-3.4
5
{"def":[1,2,3]}
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {21, 4};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::JSON;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, example, introduced_in, category};

    factory.registerFunction<FunctionSimpleJSONExtractRaw>(documentation);
    factory.registerAlias("visitParamExtractRaw", "simpleJSONExtractRaw");
}

}
