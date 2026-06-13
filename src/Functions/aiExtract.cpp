#include <Functions/FunctionBaseAI.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypeString.h>
#include <Common/Exception.h>

#include <Poco/JSON/Object.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Parser.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

class FunctionAiExtract final : public FunctionBaseAI
{
public:
    static constexpr auto name = "aiExtract";

    explicit FunctionAiExtract(ContextPtr context) : FunctionBaseAI(context) {}

    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionAiExtract>(context); }

    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors mandatory_args{
            {"collection", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), &isColumnConst, "const String"},
            {"text", static_cast<FunctionArgumentDescriptor::TypeValidator>(&FunctionBaseAI::isStringOrNullableString), nullptr, "String or Nullable(String)"},
            {"instruction_or_schema", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), &isColumnConst, "const String"},
        };
        FunctionArgumentDescriptors optional_args{
            {"temperature", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isNumber), &isColumnConst, "const Number"},
        };
        validateFunctionArguments(*this, arguments, mandatory_args, optional_args);

        return wrapReturnTypeForNullablePrompt(arguments, prompt_arg_index, std::make_shared<DataTypeString>());
    }

private:
    static constexpr float default_temp = 0.0f;
    static constexpr size_t prompt_arg_index = 1;
    static constexpr size_t instruction_arg_index = 2;
    static constexpr size_t temp_arg_idx = 3;

    String functionName() const override { return name; }

    float defaultTemperature() const override { return default_temp; }
    size_t promptArgumentIndex() const override { return prompt_arg_index; }
    size_t temperatureArgumentIndex() const override { return temp_arg_idx; }

    static bool isJSONSchema(const String & instruction)
    {
        size_t pos = instruction.find_first_not_of(" \t\n\r");
        return (pos != String::npos && instruction[pos] == '{');
    }

    String getInstruction(const ColumnsWithTypeAndName & arguments) const
    {
        return String(arguments[instruction_arg_index].column->getDataAt(0));
    }

    String buildSystemPrompt(const ColumnsWithTypeAndName & arguments) const override
    {
        auto instruction = getInstruction(arguments);
        if (isJSONSchema(instruction))
            return "Extract the following fields from the given text. Return a JSON object matching the requested schema exactly. "
                   "For any field not found in the text, use null. Field descriptions:" + instruction;
        else
            return "Extract the following information from the given text: " + instruction
                + ". Return only the extracted value, nothing else. If not found, return null.";
    }

    String buildUserMessage(const ColumnsWithTypeAndName & arguments, size_t row) const override
    {
        return String(arguments[prompt_arg_index].column->getDataAt(row));
    }

    /// Builds the OpenAI `response_format` schema object. Two shapes depending on `instruction_or_schema`:
    ///
    /// Schema mode — instruction is a JSON object like `{"name": "person name", "city": "city"}`:
    ///   {
    ///     "type": "json_schema",
    ///     "json_schema": {
    ///       "name": "extraction",
    ///       "strict": true,
    ///       "schema": {
    ///         "type": "object",
    ///         "properties": {
    ///           "name": {"type": ["string", "null"], "description": "person name"},
    ///           "city": {"type": ["string", "null"], "description": "city"}
    ///         },
    ///         "required": ["name", "city"],
    ///         "additionalProperties": false
    ///       }
    ///     }
    ///   }
    ///
    /// Instruction mode — free-form natural-language instruction:
    ///   {
    ///     "type": "json_schema",
    ///     "json_schema": {
    ///       "name": "extraction",
    ///       "strict": true,
    ///       "schema": {
    ///         "type": "object",
    ///         "properties": { "result": {"type": ["string", "null"]} },
    ///         "required": ["result"],
    ///         "additionalProperties": false
    ///       }
    ///     }
    ///   }
    Poco::JSON::Object::Ptr buildResponseFormat(const ColumnsWithTypeAndName & arguments) const override
    {
        auto instruction = getInstruction(arguments);

        Poco::JSON::Object::Ptr properties = new Poco::JSON::Object;
        Poco::JSON::Array::Ptr required = new Poco::JSON::Array;

        if (isJSONSchema(instruction))
        {
            try
            {
                Poco::JSON::Parser parser;
                auto user_obj = parser.parse(instruction).extract<Poco::JSON::Object::Ptr>();
                if (!user_obj)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "aiExtract: 'instruction_or_schema' must be a JSON object mapping field names to descriptions");

                std::vector<String> keys;
                user_obj->getNames(keys);
                for (const auto & key : keys)
                {
                    Poco::JSON::Array::Ptr type_arr = new Poco::JSON::Array;
                    type_arr->add("string");
                    type_arr->add("null");

                    Poco::JSON::Object::Ptr prop = new Poco::JSON::Object;
                    prop->set("type", type_arr);
                    prop->set("description", user_obj->getValue<String>(key));

                    properties->set(key, prop);
                    required->add(key);
                }
            }
            catch (const Poco::Exception & e)
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "aiExtract: failed to parse 'instruction_or_schema' as a JSON object of field names to string descriptions: {}",
                    e.displayText());
            }
        }
        else
        {
            Poco::JSON::Array::Ptr type_arr = new Poco::JSON::Array;
            type_arr->add("string");
            type_arr->add("null");

            Poco::JSON::Object::Ptr prop = new Poco::JSON::Object;
            prop->set("type", type_arr);

            properties->set("result", prop);
            required->add("result");
        }

        Poco::JSON::Object::Ptr schema = new Poco::JSON::Object;
        schema->set("type", "object");
        schema->set("properties", properties);
        schema->set("required", required);
        schema->set("additionalProperties", false);

        Poco::JSON::Object::Ptr json_schema = new Poco::JSON::Object;
        json_schema->set("name", "extraction");
        json_schema->set("strict", true);
        json_schema->set("schema", schema);

        Poco::JSON::Object::Ptr root = new Poco::JSON::Object;
        root->set("type", "json_schema");
        root->set("json_schema", json_schema);
        return root;
    }

    /// Unwrap a single-key `{"result": "..."}` response; in free-text mode the model is constrained to this shape
    /// and we return just the string value. If the object has multiple fields (schema mode), return the raw JSON.
    /// Parse errors are swallowed on purpose — the user will see the output anyway.
    String postProcessResponse(const String & raw) const override
    {
        if (raw.empty() || raw.front() != '{')
            return raw;

        try
        {
            Poco::JSON::Parser parser;
            auto parsed = parser.parse(raw);
            auto obj = parsed.extract<Poco::JSON::Object::Ptr>();
            if (obj && obj->size() == 1 && obj->has("result"))
            {
                auto value = obj->get("result");
                if (value.isString())
                    return value.extract<String>();
                if (value.isEmpty())
                    return {};
            }
        }
        catch (...) {} // NOLINT(bugprone-empty-catch) Ok: best-effort unwrap, see comment above.

        return raw;
    }
};

}

REGISTER_FUNCTION(AiExtract)
{
    factory.registerFunction<FunctionAiExtract>(FunctionDocumentation{
        .description = R"(
Extracts structured information from unstructured text using an LLM provider.

The third argument may be either a free-form natural-language instruction (e.g. `'the main complaint'`) or a
JSON-encoded schema of the form `'{"field_a": "description of field a", "field_b": "description of field b"}'`.

In instruction mode, the function returns the extracted value as a plain string, or an empty string if nothing was found.
In schema mode, the function returns a JSON object string whose keys match the requested schema; missing fields are `null`.

The first argument is a named collection that specifies the provider, model, endpoint, and API key.
)",
        .syntax = "aiExtract(collection, text, instruction_or_schema[, temperature])",
        .arguments = {
            {"collection", "Name of a named collection containing provider credentials and configuration.", {"String"}},
            {"text", "Text to extract information from.", {"String"}},
            {"instruction_or_schema", "Free-form extraction instruction, or a constant JSON object describing the fields to extract.", {"const String"}},
            {"temperature", "Sampling temperature controlling randomness. Default: `0.0`.", {"const Float64"}},
        },
        .returned_value = {"A single extracted value (instruction mode) or a JSON object string (schema mode). Returns the default value for the column type (empty string) if the request failed and `ai_function_throw_on_error` is disabled.", {"String"}},
        .examples = {
            {"Free-form instruction", "SELECT aiExtract('ai_credentials', 'The package arrived late and was damaged.', 'the main complaint')", "late and damaged package"},
            {"Schema extraction", R"(SELECT aiExtract('ai_credentials', review, '{"sentiment": "positive, negative or neutral", "topic": "main topic of the review"}') FROM reviews LIMIT 5)", ""},
        },
        .introduced_in = {26, 4},
        .category = FunctionDocumentation::Category::AI});

    factory.registerAlias("AIExtract", "aiExtract");
}

}
