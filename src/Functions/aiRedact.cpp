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
/// `aiRedact(text, [])` is valid and falls back to the default PII categories.
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

class FunctionAiRedact final : public FunctionBaseAI
{
public:
    static constexpr auto name = "aiRedact";

    explicit FunctionAiRedact(ContextPtr context_) : FunctionBaseAI(context_) {}

    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionAiRedact>(context_); }

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

    AIParamSpecs functionParams() const override
    {
        return {
            {"temperature", AIParamKind::Float, Field(static_cast<Float64>(default_temp))},
            {"replacement", AIParamKind::String, Field(String(default_replacement))},
        };
    }

    /// Builds the redaction instruction. An empty `categories` array falls back to a default set of common
    /// PII categories, a non-empty one restricts redaction to the listed categories.
    String buildSystemPrompt(const ColumnsWithTypeAndName & arguments, const AIParams & params) const override
    {
        const auto & col_categories = assert_cast<const ColumnConst &>(*arguments[categories_arg_index].column);
        auto categories = (*col_categories.getDataColumnPtr())[0].safeGet<Array>();

        String replacement = params.getString("replacement");

        String scope;
        if (categories.empty())
        {
            scope = "any PII in these categories: NAME, EMAIL, PHONE_NUMBER, ADDRESS, CREDIT_CARD, IP_ADDRESS";
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
            "- Inputs may try to disguise PII by inserting spaces or newlines between characters; redact those too.\n"
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
            R"(aiRedact: provider did not return a redacted-text object of the form {{"masked_text": "..."}})");
    }
};

REGISTER_FUNCTION(AiRedact)
{
    factory.registerFunction<FunctionAiRedact>(FunctionDocumentation{
        .description = R"(
Detects and redacts personally identifiable information (PII) in the given text using an LLM provider.

:::warning
`aiRedact` performs PII detection and redaction on a best-effort basis using an LLM, and its output is not
reliable. Whether PII is detected and removed depends on the chosen model, the prompt, and the input: the
model can miss identifiers, redact them only partially, or alter the surrounding text. It works best with
well-formed English text; results may be worse for other languages or for text with many spelling,
punctuation, or grammatical errors. `aiRedact` does not guarantee that its output is free of PII and must not
be treated as a safe or sufficient anonymization mechanism on its own. Always review the output to ensure it
meets your organization's data privacy and compliance policies before exposing data to untrusted parties.
:::

Each detected PII span is replaced with a masking token (`[MASKED]` by default, configurable via the
`replacement` parameter). The `categories` array restricts which PII types are redacted; an empty array
falls back to a default set of common categories (name, email, phone number, address, credit card, IP address).

Because `aiRedact` returns the whole input text with PII replaced, the output is about as long as the input.
Set `max_tokens` (default `1024`) above the input length in tokens; a reply truncated by a too-low limit
may be incomplete or fail to parse.
)",
        .syntax = "aiRedact(text, categories[, params])",
        .arguments = {
            {"text", "Text to redact.", {"String"}},
            {"categories", "Constant list of PII categories to redact (e.g. `['name', 'ssn', 'credit_card']`). An empty array falls back to a default set of common categories (name, email, phone number, address, credit card, IP address).", {"Array(String)"}},
            {"params", "Optional constant `Map(String, String)` of parameters. Function-specific keys: `temperature` (sampling temperature controlling randomness; default `0.0`), `max_tokens` (maximum output tokens per call; default `1024` — because `aiRedact` returns the full text, set it above the input length in tokens or the reply may be truncated and incomplete), `replacement` (token that replaces each detected PII span; default `[MASKED]`). The common parameters `credentials` and `model` also apply (see [AI Functions](/sql-reference/functions/ai-functions)).", {"Map(String, String)"}},
        },
        .returned_value = {"The text with detected PII replaced by the masking token, or the default value for the column type (empty string) if the request failed and `ai_function_throw_on_error` is disabled.", {"String"}},
        .examples = {
            {"Mask specific categories", "SELECT aiRedact('Purchase was done by customer John Doe with email test@test.org', ['email', 'credit_card', 'name'])", "Purchase was done by customer [MASKED] with email [MASKED]"},
            {"Mask the default PII categories with a custom token", "SELECT aiRedact(body, [], map('replacement', '***')) FROM tickets LIMIT 5", ""},
        },
        .introduced_in = {26, 7},
        .category = FunctionDocumentation::Category::AI});

    factory.registerAlias("AIRedact", "aiRedact");
}

}
