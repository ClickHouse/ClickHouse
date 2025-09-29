#include <Functions/FunctionSQLJSON.h>
#include <Functions/FunctionFactory.h>


namespace DB
{

REGISTER_FUNCTION(SQLJSON)
{
    /// JSON_EXISTS
    {
        FunctionDocumentation::Description description = R"(
If the value exists in the JSON document, `1` will be returned.
If the value does not exist, `0` will be returned.
        )";
        FunctionDocumentation::Syntax syntax = "JSON_EXISTS(json, path)";
        FunctionDocumentation::Arguments arguments = {
            {"json", "A string with valid JSON.", {"String"}},
            {"path", "A string representing the path.", {"String"}}
        };
        FunctionDocumentation::ReturnedValue returned_value = {"Returns `1` if the value exists in the JSON document, otherwise `0`.", {"UInt8"}};
        FunctionDocumentation::Examples examples = {
        {
            "Usage example",
            R"(
SELECT JSON_EXISTS('{"hello":1}', '$.hello');
SELECT JSON_EXISTS('{"hello":{"world":1}}', '$.hello.world');
SELECT JSON_EXISTS('{"hello":["world"]}', '$.hello[*]');
SELECT JSON_EXISTS('{"hello":["world"]}', '$.hello[0]');
            )",
            R"(
┌─JSON_EXISTS(⋯ '$.hello')─┐
│                        1 │
└──────────────────────────┘
┌─JSON_EXISTS(⋯llo.world')─┐
│                        1 │
└──────────────────────────┘
┌─JSON_EXISTS(⋯.hello[*]')─┐
│                        1 │
└──────────────────────────┘
┌─JSON_EXISTS(⋯.hello[0]')─┐
│                        1 │
└──────────────────────────┘
            )"
        }
        };
        FunctionDocumentation::IntroducedIn introduced_in = {21, 8};
        FunctionDocumentation::Category category = FunctionDocumentation::Category::JSON;
        FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};
        factory.registerFunction<FunctionSQLJSON<NameJSONExists, JSONExistsImpl>>(documentation);
    }

    /// JSON_QUERY
    {
        FunctionDocumentation::Description description = R"(
Parses a JSON and extract a value as a JSON array or JSON object.
If the value does not exist, an empty string will be returned.
        )";
        FunctionDocumentation::Syntax syntax = "JSON_QUERY(json, path)";
        FunctionDocumentation::Arguments arguments = {
            {"json", "A string with valid JSON.", {"String"}},
            {"path", "A string representing the path.", {"String"}}
        };
        FunctionDocumentation::ReturnedValue returned_value = {"Returns the extracted JSON array or JSON object as a string, or an empty string if the value does not exist.", {"String"}};
        FunctionDocumentation::Examples examples = {
        {
            "Usage example",
            R"(
SELECT JSON_QUERY('{"hello":"world"}', '$.hello');
SELECT JSON_QUERY('{"array":[[0, 1, 2, 3, 4, 5], [0, -1, -2, -3, -4, -5]]}', '$.array[*][0 to 2, 4]');
SELECT JSON_QUERY('{"hello":2}', '$.hello');
SELECT toTypeName(JSON_QUERY('{"hello":2}', '$.hello'));
            )",
            R"(
["world"]
[0, 1, 4, 0, -1, -4]
[2]
String
            )"
        }
        };
        FunctionDocumentation::IntroducedIn introduced_in = {21, 8};
        FunctionDocumentation::Category category = FunctionDocumentation::Category::JSON;
        FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};
        factory.registerFunction<FunctionSQLJSON<NameJSONQuery, JSONQueryImpl>>(documentation);
    }

    /// JSON_VALUE
    {
        FunctionDocumentation::Description description = R"(
Parses a JSON and extract a value as a JSON scalar. If the value does not exist, an empty string will be returned by default.

This function is controlled by the following settings:
- by SET `function_json_value_return_type_allow_nullable` = `true`, `NULL` will be returned. If the value is complex type (such as: struct, array, map), an empty string will be returned by default.
- by SET `function_json_value_return_type_allow_complex` = `true`, the complex value will be returned.
        )";
        FunctionDocumentation::Syntax syntax = "JSON_VALUE(json, path)";
        FunctionDocumentation::Arguments arguments = {
            {"json", "A string with valid JSON.", {"String"}},
            {"path", "A string representing the path.", {"String"}}
        };
        FunctionDocumentation::ReturnedValue returned_value = {"Returns the extracted JSON scalar as a string, or an empty string if the value does not exist.", {"String"}};
        FunctionDocumentation::Examples examples = {
        {
            "Usage example",
            R"(
SELECT JSON_VALUE('{"hello":"world"}', '$.hello');
SELECT JSON_VALUE('{"array":[[0, 1, 2, 3, 4, 5], [0, -1, -2, -3, -4, -5]]}', '$.array[*][0 to 2, 4]');
SELECT JSON_VALUE('{"hello":2}', '$.hello');
SELECT JSON_VALUE('{"hello":"world"}', '$.b') settings function_json_value_return_type_allow_nullable=true;
            )",
            R"(
world
0
2
ᴺᵁᴸᴸ
            )"
        }
        };
        FunctionDocumentation::IntroducedIn introduced_in = {21, 11};
        FunctionDocumentation::Category category = FunctionDocumentation::Category::JSON;
        FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};
        factory.registerFunction<FunctionSQLJSON<NameJSONValue, JSONValueImpl>>(documentation);
    }
}

}
