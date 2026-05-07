#include <Functions/FunctionBaseAI.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/IDataType.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>

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

bool isArrayOfStrings(const IDataType & type)
{
    const auto * array_type = typeid_cast<const DataTypeArray *>(&type);
    return (array_type && isString(array_type->getNestedType()));
}

}

class FunctionAiClassify final : public FunctionBaseAI
{
public:
    static constexpr auto name = "aiClassify";

    explicit FunctionAiClassify(ContextPtr context) : FunctionBaseAI(context) {}

    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionAiClassify>(context); }

    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors mandatory_args{
            {"collection", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), &isColumnConst, "const String"},
            {"text", static_cast<FunctionArgumentDescriptor::TypeValidator>(&FunctionBaseAI::isStringOrNullableString), nullptr, "String or Nullable(String)"},
            {"categories", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isArrayOfStrings), &isColumnConst, "const Array(String)"},
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
    static constexpr size_t categories_arg_index = 2;
    static constexpr size_t temp_arg_idx = 3;

    String functionName() const override { return name; }

    float defaultTemperature() const override { return default_temp; }
    size_t promptArgumentIndex() const override { return prompt_arg_index; }
    size_t temperatureArgumentIndex() const override { return temp_arg_idx; }

    void checkSanityBeforeExecuteImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
    {
        /// An empty category list would produce `"enum": []` in the response-format schema, which no provider can
        /// satisfy. Fail early with a deterministic local exception instead of waiting for a provider-side error.
        if (input_rows_count)
        {
            const auto & col_categories = assert_cast<const ColumnConst &>(*arguments[categories_arg_index].column);
            auto categories = (*col_categories.getDataColumnPtr())[0].safeGet<Array>();
            if (categories.empty())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "aiClassify: 'categories' must contain at least one label");
        }
    }

    String buildSystemPrompt(const ColumnsWithTypeAndName & arguments) const override
    {
        const auto & col_categories = assert_cast<const ColumnConst &>(*arguments[categories_arg_index].column);
        auto categories = (*col_categories.getDataColumnPtr())[0].safeGet<Array>();

        String labels;
        bool first = true;
        for (const auto & category : categories)
        {
            if (!first)
                labels += ", ";
            first = false;
            labels += category.safeGet<String>();
        }

        return "You are a text classifier. Classify the given text into exactly one of these categories: "
            + labels
            + ". Respond with ONLY the category label, nothing else.";
    }

    String buildUserMessage(const ColumnsWithTypeAndName & arguments, size_t row) const override
    {
        return String(arguments[prompt_arg_index].column->getDataAt(row));
    }

    /// Builds the OpenAI `response_format` schema object constraining the model to output one of the
    /// provided category labels. Given `categories = ['positive', 'negative', 'neutral']`:
    ///   {
    ///     "type": "json_schema",
    ///     "json_schema": {
    ///       "name": "classification",
    ///       "strict": true,
    ///       "schema": {
    ///         "type": "object",
    ///         "properties": {
    ///           "category": {"type": "string", "enum": ["positive", "negative", "neutral"]}
    ///         },
    ///         "required": ["category"],
    ///         "additionalProperties": false
    ///       }
    ///     }
    ///   }
    Poco::JSON::Object::Ptr buildResponseFormat(const ColumnsWithTypeAndName & arguments) const override
    {
        Poco::JSON::Array::Ptr enum_array = new Poco::JSON::Array;

        const auto & col_categories = assert_cast<const ColumnConst &>(*arguments[categories_arg_index].column);
        auto categories = (*col_categories.getDataColumnPtr())[0].safeGet<Array>();
        for (const auto & category : categories)
            enum_array->add(category.safeGet<String>());

        Poco::JSON::Object::Ptr category_prop = new Poco::JSON::Object;
        category_prop->set("type", "string");
        category_prop->set("enum", enum_array);

        Poco::JSON::Object::Ptr properties = new Poco::JSON::Object;
        properties->set("category", category_prop);

        Poco::JSON::Array::Ptr required = new Poco::JSON::Array;
        required->add("category");

        Poco::JSON::Object::Ptr schema = new Poco::JSON::Object;
        schema->set("type", "object");
        schema->set("properties", properties);
        schema->set("required", required);
        schema->set("additionalProperties", false);

        Poco::JSON::Object::Ptr json_schema = new Poco::JSON::Object;
        json_schema->set("name", "classification");
        json_schema->set("strict", true);
        json_schema->set("schema", schema);

        Poco::JSON::Object::Ptr root = new Poco::JSON::Object;
        root->set("type", "json_schema");
        root->set("json_schema", json_schema);
        return root;
    }

    /// The provider is instructed to return `{"category": "<label>"}` via the JSON-schema response format, so in
    /// practice the response parses as an object with a `category` key. Anything else (model ignored the schema,
    /// returned a bare label, or returned truncated/invalid JSON) falls through to `raw_response`. Parse errors are
    /// swallowed on purpose — the user will see the output anyway.
    String postProcessResponse(const String & raw_response) const override
    {
        if (raw_response.empty() || raw_response.front() != '{')
            return raw_response;

        try
        {
            Poco::JSON::Parser parser;
            auto parsed = parser.parse(raw_response);
            auto obj = parsed.extract<Poco::JSON::Object::Ptr>();
            if (obj && obj->has("category"))
                return obj->getValue<String>("category");
        }
        catch (...) {} // NOLINT(bugprone-empty-catch) Ok: best-effort unwrap, see comment above.

        return raw_response;
    }
};

REGISTER_FUNCTION(AiClassify)
{
    factory.registerFunction<FunctionAiClassify>(FunctionDocumentation{
        .description = R"(
Classifies the given text into one of the provided categories using an LLM provider.

The function sends the text together with a fixed classification prompt and a JSON-schema response format
constraining the model to return exactly one of the supplied labels. When the response is returned as a JSON
object of the form `{"category": "..."}`, the label is unwrapped and the label string is returned.

The first argument is a named collection that specifies the provider, model, endpoint, and API key.
)",
        .syntax = "aiClassify(collection, text, categories[, temperature])",
        .arguments = {
            {"collection", "Name of a named collection containing provider credentials and configuration.", {"String"}},
            {"text", "Text to classify.", {"String"}},
            {"categories", "Constant list of candidate category labels.", {"Array(String)"}},
            {"temperature", "Sampling temperature controlling randomness. Default: `0.0`.", {"Float64"}},
        },
        .returned_value = {"One of the provided category labels, or the default value for the column type (empty string) if the request failed and `ai_function_throw_on_error` is disabled.", {"String"}},
        .examples = {
            {"Classify sentiment", "SELECT aiClassify('ai_credentials', 'I love this product!', ['positive', 'negative', 'neutral'])", "positive"},
            {"Classify a column", "SELECT body, aiClassify('ai_credentials', body, ['bug', 'question', 'feature']) AS kind FROM issues LIMIT 5", ""},
        },
        .introduced_in = {26, 4},
        .category = FunctionDocumentation::Category::AI});

    factory.registerAlias("AIClassify", "aiClassify");
}

}
