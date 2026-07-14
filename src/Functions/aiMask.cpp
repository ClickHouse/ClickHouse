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
    extern const int MALFORMED_AI_PROVIDER_RESPONSE;
}

namespace
{

/// Accepts `Array(String)` and the empty-array literal `[]`.
/// `aiMask(text, [])` redacts all detected PII.
bool isArrayOfStringsOrEmpty(const IDataType & type)
{
    const auto * array_type = typeid_cast<const DataTypeArray *>(&type);
    if (!array_type)
        return false;
    const auto & nested = array_type->getNestedType();
    return isString(nested) || isNothing(nested);
}

/// Token that replaces each detected PII span, unless overridden via the `replacement` parameter.
constexpr auto default_replacement = "[MASKED]";

}

class FunctionAiMask final : public FunctionBaseAI
{
public:
    static constexpr auto name = "aiMask";

    explicit FunctionAiMask(ContextPtr context_) : FunctionBaseAI(context_) {}

    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionAiMask>(context_); }

    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors mandatory_args{
            {"text", static_cast<FunctionArgumentDescriptor::TypeValidator>(&FunctionBaseAI::isStringOrNullableString), nullptr, "String or Nullable(String)"},
            {"categories", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isArrayOfStringsOrEmpty), &isColumnConst, "const Array(String)"},
        };
        FunctionArgumentDescriptors optional_args{
            {"params", static_cast<FunctionArgumentDescriptor::TypeValidator>(&FunctionBaseAI::isStringToStringMap), &isColumnConst, "const Map(String, String)"},
        };
        validateFunctionArguments(*this, arguments, mandatory_args, optional_args);

        return wrapReturnTypeForNullablePrompt(arguments, 0, std::make_shared<DataTypeString>());
    }

private:
    static constexpr float default_temp = 0.0f;
    static constexpr size_t categories_arg_index = 1;

    String functionName() const override { return name; }

    /// The query fails on any error regardless of `ai_function_throw_on_error`.
    bool failClosedOnError() const override { return true; }

    AIParamSpecs functionParams() const override
    {
        return {
            {"temperature", AIParamKind::Float, Field(static_cast<Float64>(default_temp))},
            {"replacement", AIParamKind::String, Field(String(default_replacement))},
        };
    }

    /// Builds the redaction instruction. An empty `categories` array means "redact every PII category",
    /// a non-empty one restricts redaction to the listed categories.
    String buildSystemPrompt(const ColumnsWithTypeAndName & arguments, const AIParams & params) const override
    {
        const auto & col_categories = assert_cast<const ColumnConst &>(*arguments[categories_arg_index].column);
        auto categories = (*col_categories.getDataColumnPtr())[0].safeGet<Array>();

        String replacement = params.getString("replacement");

        String scope;
        if (categories.empty())
        {
            scope = "any category of PII";
        }
        else
        {
            String labels;
            bool first = true;
            for (const auto & category : categories)
            {
                if (!first)
                    labels += ", ";
                first = false;
                labels += category.safeGet<String>();
            }
            scope = "only PII of these categories: " + labels;
        }

        return
            "You are a precise PII redaction engine. Redact " + scope + " in the user's text.\n"
            "Rules:\n"
            "- Replace each detected PII span, in full, with the exact literal token " + replacement + ".\n"
            "- Redact the complete value (e.g. a whole name or email address), not just part of it.\n"
            "- Keep every other character identical to the input, including wording, casing, punctuation, and whitespace.\n"
            "- Do not add, remove, reorder, translate, summarize, or comment on anything.\n"
            "- If the text contains no PII to redact, return it unchanged.";
    }

    String buildUserMessage(const ColumnsWithTypeAndName & arguments, size_t row) const override
    {
        return String(arguments[0].column->getDataAt(row));
    }

    /// Constrains the model to return the redacted text as a single-field JSON object:
    ///   {
    ///     "type": "json_schema",
    ///     "json_schema": {
    ///       "name": "redaction",
    ///       "strict": true,
    ///       "schema": {
    ///         "type": "object",
    ///         "properties": { "masked_text": {"type": "string"} },
    ///         "required": ["masked_text"],
    ///         "additionalProperties": false
    ///       }
    ///     }
    ///   }
    Poco::JSON::Object::Ptr buildResponseFormat(const ColumnsWithTypeAndName &) const override
    {
        Poco::JSON::Object::Ptr masked_prop = new Poco::JSON::Object;
        masked_prop->set("type", "string");

        Poco::JSON::Object::Ptr properties = new Poco::JSON::Object;
        properties->set("masked_text", masked_prop);

        Poco::JSON::Array::Ptr required = new Poco::JSON::Array;
        required->add("masked_text");

        Poco::JSON::Object::Ptr schema = new Poco::JSON::Object;
        schema->set("type", "object");
        schema->set("properties", properties);
        schema->set("required", required);
        schema->set("additionalProperties", false);

        Poco::JSON::Object::Ptr json_schema = new Poco::JSON::Object;
        json_schema->set("name", "redaction");
        json_schema->set("strict", true);
        json_schema->set("schema", schema);

        Poco::JSON::Object::Ptr root = new Poco::JSON::Object;
        root->set("type", "json_schema");
        root->set("json_schema", json_schema);
        return root;
    }

    /// Any response that does not parse into a string `masked_text` is rejected.
    String postProcessResponse(const String & raw_response) const override
    {
        try
        {
            Poco::JSON::Parser parser;
            auto obj = parser.parse(raw_response).extract<Poco::JSON::Object::Ptr>();
            if (obj && obj->has("masked_text"))
            {
                auto value = obj->get("masked_text");
                if (value.isString())
                    return value.extract<String>();
            }
        }
        catch (const Poco::Exception &) {} // NOLINT(bugprone-empty-catch) Ok: fall through to the throw below.

        throw Exception(ErrorCodes::MALFORMED_AI_PROVIDER_RESPONSE,
            "aiMask: provider did not return a redacted-text object of the form {{\"masked_text\": \"...\"}}");
    }
};

REGISTER_FUNCTION(AiMask)
{
    factory.registerFunction<FunctionAiMask>(FunctionDocumentation{
        .description = R"(
Detects and redacts personally identifiable information (PII) in the given text using an LLM provider.

Each detected PII span is replaced with a masking token (`[MASKED]` by default, configurable via the
`replacement` parameter). The `categories` array restricts which PII types are redacted; an empty array
means "redact every category the model can detect".

Because `aiMask` returns the whole input text with PII replaced, the output is about as long as the input.
Set `max_tokens` (default `1024`) above the input length in tokens. If the reply is truncated because the
limit is too low, it no longer parses as a valid response and the query fails.

`aiMask` is fail-closed: any error (a provider failure with retries exhausted, or a malformed or truncated
response) aborts the whole query regardless of `ai_function_throw_on_error`. The one exception is quota
exhaustion: when `ai_function_throw_on_quota_exceeded = 0`, rows over the per-query quota are left
unprocessed and yield an empty string instead of throwing. An empty string never leaks PII, so this stays
safe, but such rows are silently emptied rather than redacted.
)",
        .syntax = "aiMask(text, categories[, params])",
        .arguments = {
            {"text", "Text to redact.", {"String"}},
            {"categories", "Constant list of PII categories to redact (e.g. `['name', 'ssn', 'credit_card']`). An empty array redacts all detected PII.", {"Array(String)"}},
            {"params", "Optional constant `Map(String, String)` of parameters. Function-specific keys: `temperature` (sampling temperature controlling randomness; default `0.0`), `max_tokens` (maximum output tokens per call; default `1024` — because `aiMask` returns the full text, set it above the input length in tokens or the reply is truncated and the query fails), `replacement` (token that replaces each detected PII span; default `[MASKED]`). The common parameters `credentials` and `model` also apply (see [AI Functions](/sql-reference/functions/ai-functions)).", {"Map(String, String)"}},
        },
        .returned_value = {"The text with detected PII replaced by the masking token.", {"String"}},
        .examples = {
            {"Mask specific categories", "SELECT aiMask('Purchase was done by customer John Doe with email test@test.org', ['email', 'credit_card', 'name'])", "Purchase was done by customer [MASKED] with email [MASKED]"},
            {"Mask all detected PII with a custom token", "SELECT aiMask(body, [], map('replacement', '***')) FROM tickets LIMIT 5", ""},
        },
        .introduced_in = {26, 7},
        .category = FunctionDocumentation::Category::AI});

    factory.registerAlias("AIMask", "aiMask");
}

}
