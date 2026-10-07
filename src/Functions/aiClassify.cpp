#include <Functions/FunctionBaseAI.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnTuple.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>

#include <Poco/JSON/Object.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Parser.h>

#include <algorithm>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

bool isArrayOfStringsOrStringToStringMap(const IDataType & type)
{
    const auto * array_type = typeid_cast<const DataTypeArray *>(&type);
    return (array_type && isString(array_type->getNestedType())) || FunctionBaseAI::isStringToStringMap(type);
}

/// Labels with their descriptions. The description is empty when `categories` is an `Array`.
using Categories = VectorWithMemoryTracking<std::pair<String, String>>;

}

class FunctionAiClassify final : public FunctionBaseAI
{
public:
    static constexpr auto name = "aiClassify";

    explicit FunctionAiClassify(ContextPtr context_) : FunctionBaseAI(context_) {}

    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionAiClassify>(context_); }

    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors mandatory_args{
            {"text", static_cast<FunctionArgumentDescriptor::TypeValidator>(&FunctionBaseAI::isStringOrNullableString), nullptr, "String"},
            {"categories", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isArrayOfStringsOrStringToStringMap), &isColumnConst, "const Array(String) or const Map(String, String)"},
        };
        FunctionArgumentDescriptors optional_args{
            {"params", static_cast<FunctionArgumentDescriptor::TypeValidator>(&FunctionBaseAI::isStringToStringMap), &isColumnConst, "const Map(String, String)"},
        };
        validateFunctionArguments(*this, arguments, mandatory_args, optional_args);

        if (!returnConfidence(arguments))
            return wrapReturnTypeForNullablePrompt(arguments, 0, std::make_shared<DataTypeString>());

        /// `Nullable(Tuple)` is not allowed, so a Nullable `text` makes the tuple elements Nullable.
        DataTypePtr label_type = wrapReturnTypeForNullablePrompt(arguments, 0, std::make_shared<DataTypeString>());
        DataTypePtr confidence_type = wrapReturnTypeForNullablePrompt(arguments, 0, std::make_shared<DataTypeFloat64>());
        return std::make_shared<DataTypeTuple>(DataTypes{label_type, confidence_type}, Strings{"label", "confidence"});
    }

private:
    static constexpr float default_temp = 0.0f;
    static constexpr size_t categories_arg_index = 1;
    static constexpr size_t params_arg_index = 2;
    static constexpr auto return_confidence_key = "return_confidence";

    size_t firstParamsArgumentIndex() const override { return params_arg_index; }

    AIParamSpecs functionParams() const override
    {
        return {
            {"temperature", AIParamKind::Float, Field(static_cast<Float64>(default_temp))},
            {return_confidence_key, AIParamKind::UInt, Field(UInt64(0))},
        };
    }

    /// Reads `return_confidence` from the parameter map. `resolveAIParams` runs only at execution, but the
    /// key determines the return type.
    static bool returnConfidence(const ColumnsWithTypeAndName & arguments)
    {
        if (arguments.size() <= params_arg_index)
            return false;

        const auto & col_params = assert_cast<const ColumnConst &>(*arguments[params_arg_index].column);
        const Map & map = (*col_params.getDataColumnPtr())[0].safeGet<Map>();
        for (const auto & element : map)
        {
            const Tuple & kv = element.safeGet<Tuple>();
            if (kv[0].safeGet<String>() != return_confidence_key)
                continue;
            const String & value = kv[1].safeGet<String>();
            if (value == "1")
                return true;
            if (value == "0")
                return false;
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "aiClassify: '{}' must be '0' or '1', got '{}'", return_confidence_key, value);
        }
        return false;
    }

    static bool hasDescriptions(const ColumnsWithTypeAndName & arguments)
    {
        return isStringToStringMap(*arguments[categories_arg_index].type);
    }

    static Categories parseCategories(const ColumnsWithTypeAndName & arguments)
    {
        const auto & col_categories = assert_cast<const ColumnConst &>(*arguments[categories_arg_index].column);
        const Field field = (*col_categories.getDataColumnPtr())[0];

        Categories categories;
        if (hasDescriptions(arguments))
        {
            for (const auto & element : field.safeGet<Map>())
            {
                const Tuple & kv = element.safeGet<Tuple>();
                categories.emplace_back(kv[0].safeGet<String>(), kv[1].safeGet<String>());
            }

            VectorWithMemoryTracking<std::string_view> labels;
            for (const auto & [label, _] : categories)
                labels.push_back(label);
            std::sort(labels.begin(), labels.end());
            if (auto it = std::adjacent_find(labels.begin(), labels.end()); it != labels.end())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "aiClassify: duplicate label '{}' in 'categories'", *it);
        }
        else
        {
            for (const auto & label : field.safeGet<Array>())
                categories.emplace_back(label.safeGet<String>(), "");
        }

        /// An empty category list would produce `"enum": []` in the response-format schema, which no provider can
        /// satisfy. Fail early with a deterministic local exception instead of waiting for a provider-side error.
        if (categories.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "aiClassify: 'categories' must contain at least one label");

        return categories;
    }

    void checkSanityBeforeExecuteImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t /*input_rows_count*/) const override
    {
        parseCategories(arguments);
    }

    void checkProvider(const IAIProvider & provider, const String & provider_name, const ColumnsWithTypeAndName & arguments) const override
    {
        if (returnConfidence(arguments) && !provider.supportsDecisions())
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "aiClassify: '{}' requires a provider that returns probabilities ('typesafe'), got '{}'",
                return_confidence_key, provider_name);
    }

    bool supportsDecisions() const override { return true; }

    /// An `Array` gives each label itself as the description.
    AIDecisionQuestion buildDecisionQuestion(const ColumnsWithTypeAndName & arguments) const override
    {
        AIDecisionQuestion question;
        question.key = "category";
        question.type = AIDecisionQuestionType::Choice;
        question.instructions = "Classify the text into exactly one of the categories.";
        for (auto & [label, description] : parseCategories(arguments))
        {
            String criterion = description.empty() ? label : std::move(description);
            question.criteria.emplace_back(std::move(label), std::move(criterion));
        }
        return question;
    }

    void insertDecisionResult(IColumn & column, const AIDecisionAnswer & answer) const override
    {
        if (typeid_cast<const ColumnTuple *>(&column))
            column.insert(Tuple{Field(answer.choice), Field(answer.confidence)});
        else
            column.insertData(answer.choice.data(), answer.choice.size());
    }

    String buildSystemPrompt(const ColumnsWithTypeAndName & arguments, const AIParams &) const override
    {
        auto categories = parseCategories(arguments);

        if (hasDescriptions(arguments))
        {
            String labels;
            for (const auto & [label, description] : categories)
                labels += "\n" + label + ": " + description;

            return "You are a text classifier. Classify the given text into exactly one of these categories, "
                "given as `label: description` lines:"
                + labels
                + "\nRespond with ONLY the category label, nothing else.";
        }

        String labels;
        bool first = true;
        for (const auto & [label, _] : categories)
        {
            if (!first)
                labels += ", ";
            first = false;
            labels += label;
        }

        return "You are a text classifier. Classify the given text into exactly one of these categories: "
            + labels
            + ". Respond with ONLY the category label, nothing else.";
    }

    String buildUserMessage(const ColumnsWithTypeAndName & arguments, size_t row) const override
    {
        return String(arguments[0].column->getDataAt(row));
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

        for (const auto & [label, _] : parseCategories(arguments))
            enum_array->add(label);

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
Classifies the given text into one of the provided categories using an LLM provider or the TypeSafe Jev decision model.

Credentials (a named collection specifying the provider, model, endpoint, and optionally an API key)
are taken from the `credentials` key of the optional parameter map, or from the
`ai_function_text_default_credentials` setting when the map omits it.

With `provider = 'typesafe'`, each row is sent to the decision endpoint, and `temperature` and `max_tokens` are not used.
)",
        .syntax = "aiClassify(text, categories[, params])",
        .arguments = {
            {"text", "Text to classify.", {"String"}},
            {"categories", "Constant list of candidate category labels, or a constant map of label to label description.", {"Array(String)", "Map(String, String)"}},
            {"params", "Optional constant `Map(String, String)` of parameters. Function-specific keys: `temperature` (sampling temperature controlling randomness; default `0.0`), `max_tokens` (maximum output tokens per call; default `1024`), `return_confidence` (`'1'` returns the confidence of the label, supported only by `provider = 'typesafe'`; default `'0'`). The common parameters `credentials` and `model` also apply (see [AI Functions](/reference/functions/regular-functions/ai-functions)).", {"Map(String, String)"}},
        },
        .returned_value = {"One of the provided category labels, or the default value for the column type (empty string) if the request failed and `ai_function_throw_on_error` is disabled. With `return_confidence = '1'`, a `Tuple(label String, confidence Float64)`; for a failed request, `('', 0)`. For a `Nullable` text, the tuple elements are `Nullable`.", {"String", "Tuple(label String, confidence Float64)"}},
        .examples = {
            {"Classify sentiment", "SELECT aiClassify('I love this product!', ['positive', 'negative', 'neutral'])", "positive"},
            {"Classify a column with explicit credentials", "CREATE TABLE issues (body String) ENGINE = Memory;\nINSERT INTO issues VALUES ('The application exits unexpectedly after login.');\nSELECT body, aiClassify(body, ['bug', 'question', 'feature'], map('credentials', 'ai_text_credentials')) AS kind FROM issues LIMIT 5", ""},
            {"Classify with label descriptions and confidence using TypeSafe Jev", "CREATE NAMED COLLECTION jev_creds AS provider = 'typesafe', endpoint = 'https://inference-internal.clickhouse.cloud/v1/systemone', model = 'jev-latest', api_key = '...';\nSELECT aiClassify('The application exits unexpectedly after login.', map('bug', 'A software defect', 'question', 'A usage question'), map('credentials', 'jev_creds', 'return_confidence', '1'))", "('bug',0.98)"},
        },
        .introduced_in = {26, 4},
        .category = FunctionDocumentation::Category::AI});

    factory.registerAlias("AIClassify", "aiClassify");
}

}
