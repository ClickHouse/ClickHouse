#include <Functions/FunctionBaseAI.h>
#include <Functions/FunctionFactory.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeNullable.h>
#include <Common/Exception.h>
#include <Poco/JSON/Parser.h>
#include <Poco/JSON/Object.h>
#include <sstream>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

class FunctionAiExtract final : public FunctionBaseAI
{
public:
    static constexpr auto name = "aiExtract";

    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionAiExtract>(context); }
    explicit FunctionAiExtract(ContextPtr context) : FunctionBaseAI(context) {}

    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() < 2 || arguments.size() > 4)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} requires 2-4 arguments: [collection,] text, instruction_or_schema[, temperature]", name);
        return std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
    }

protected:
    String functionName() const override { return name; }
    float defaultTemperature() const override { return 0.0f; }

    bool isJSONSchema(const String & instruction) const
    {
        return !instruction.empty() && instruction.front() == '{';
    }

    String buildSystemPrompt(const ColumnsWithTypeAndName & arguments) const override
    {
        size_t idx = getFirstDataArgIndex(arguments);
        String instruction(arguments[idx + 1].column->getDataAt(0));

        if (isJSONSchema(instruction))
        {
            return "Extract the following fields from the given text. Return a JSON object "
                   "matching the requested schema exactly. For any field not found in the text, "
                   "use null. Field descriptions:\n" + instruction;
        }
        return "Extract the following information from the given text: " + instruction
               + ". Return only the extracted value, nothing else. If not found, return null.";
    }

    String buildUserMessage(const ColumnsWithTypeAndName & arguments, size_t row) const override
    {
        size_t idx = getFirstDataArgIndex(arguments);
        return String(arguments[idx].column->getDataAt(row));
    }

    String buildResponseFormatJSON(const ColumnsWithTypeAndName & arguments) const override
    {
        size_t idx = getFirstDataArgIndex(arguments);
        String instruction(arguments[idx + 1].column->getDataAt(0));

        if (isJSONSchema(instruction))
        {
            Poco::JSON::Parser parser;
            auto parsed = parser.parse(instruction);
            const auto & user_obj = parsed.extract<Poco::JSON::Object::Ptr>();

            Poco::JSON::Object::Ptr properties = new Poco::JSON::Object;
            Poco::JSON::Array::Ptr required_arr = new Poco::JSON::Array;

            std::vector<String> keys;
            user_obj->getNames(keys);

            for (const auto & key : keys)
            {
                Poco::JSON::Object::Ptr prop = new Poco::JSON::Object;
                Poco::JSON::Array::Ptr type_arr = new Poco::JSON::Array;
                type_arr->add("string");
                type_arr->add("null");
                prop->set("type", type_arr);
                prop->set("description", user_obj->getValue<String>(key));
                properties->set(key, prop);
                required_arr->add(key);
            }

            Poco::JSON::Object::Ptr schema = new Poco::JSON::Object;
            schema->set("type", "object");
            schema->set("properties", properties);
            schema->set("required", required_arr);
            schema->set("additionalProperties", false);

            Poco::JSON::Object::Ptr json_schema = new Poco::JSON::Object;
            json_schema->set("name", "extraction");
            json_schema->set("strict", true);
            json_schema->set("schema", schema);

            Poco::JSON::Object::Ptr root = new Poco::JSON::Object;
            root->set("type", "json_schema");
            root->set("json_schema", json_schema);

            std::ostringstream oss; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
            root->stringify(oss);
            return oss.str();
        }

        return R"({"type":"json_schema","json_schema":{"name":"extraction","strict":true,"schema":{"type":"object","properties":{"result":{"type":["string","null"]}},"required":["result"],"additionalProperties":false}}})";
    }

    String postProcessResponse(const String & raw) const override
    {
        if (raw.empty())
            return raw;

        if (raw.front() != '{')
            return raw;

        auto pos = raw.find("\"result\"");
        if (pos != String::npos && raw.find("\"result\"", pos + 8) == String::npos)
        {
            auto colon = raw.find(':', pos + 8);
            if (colon != String::npos)
            {
                size_t val_start = colon + 1;
                while (val_start < raw.size() && raw[val_start] == ' ')
                    ++val_start;
                if (val_start < raw.size() && raw[val_start] == '"')
                {
                    ++val_start;
                    auto val_end = raw.find('"', val_start);
                    if (val_end != String::npos)
                        return raw.substr(val_start, val_end - val_start);
                }
            }
        }

        return raw;
    }
};

}

REGISTER_FUNCTION(AiExtract)
{
    factory.registerFunction<FunctionAiExtract>(FunctionDocumentation{
        .description = "Extracts structured information from unstructured text using an LLM.",
        .syntax = "aiExtract([collection,] text, instruction_or_schema[, temperature])",
        .arguments = {{"text", "Input text"}, {"instruction_or_schema", "Extraction instruction or JSON schema"}},
        .returned_value = {"Extracted value as String.", {"String"}},
        .examples = {{"basic", "SELECT aiExtract(body, 'main complaint') FROM reviews", ""}},
        .introduced_in = {26, 4},
        .category = FunctionDocumentation::Category::Other});
}

}
