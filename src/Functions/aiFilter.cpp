#include <Functions/FunctionBaseAI.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>
#include <Common/StringUtils.h>
#include <cctype>
#include <ranges>

#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

class FunctionAiFilter final : public FunctionBaseAI
{
public:
    static constexpr auto name = "aiFilter";

    explicit FunctionAiFilter(ContextPtr context_) : FunctionBaseAI(context_) {}

    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionAiFilter>(context_); }

    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors mandatory_args{
            {"text", static_cast<FunctionArgumentDescriptor::TypeValidator>(&FunctionBaseAI::isStringOrNullableString), nullptr, "String"},
            {"condition", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), &isColumnConst, "const String"},
        };
        FunctionArgumentDescriptors optional_args{
            {"params", static_cast<FunctionArgumentDescriptor::TypeValidator>(&FunctionBaseAI::isStringToStringMap), &isColumnConst, "const Map(String, String)"},
        };
        validateFunctionArguments(*this, arguments, mandatory_args, optional_args);

        return wrapReturnTypeForNullablePrompt(arguments, 0, std::make_shared<DataTypeUInt8>());
    }

private:
    static constexpr float default_temp = 0.0f;
    static constexpr size_t condition_arg_index = 1;

    AIParamSpecs functionParams() const override
    {
        return {{"temperature", AIParamKind::Float, Field(static_cast<Float64>(default_temp))}};
    }

    void checkSanityBeforeExecuteImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t /*input_rows_count*/) const override
    {
        auto condition = arguments[condition_arg_index].column->getDataAt(0);
        if (std::ranges::all_of(condition, isWhitespaceASCII))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "aiFilter: 'condition' must not be empty");
    }

    String buildSystemPrompt(const ColumnsWithTypeAndName & arguments, const AIParams &) const override
    {
        auto condition = String(arguments[condition_arg_index].column->getDataAt(0));
        return "You are a boolean text filter. Decide whether the given text satisfies this condition: "
            + condition + ". Respond with ONLY true or false, nothing else.";
    }

    String buildUserMessage(const ColumnsWithTypeAndName & arguments, size_t row) const override
    {
        return String(arguments[0].column->getDataAt(row));
    }

    /// Builds the OpenAI `response_format` schema object constraining the model to output a single
    /// boolean `match` field, e.g.:
    ///   {
    ///     "type": "json_schema",
    ///     "json_schema": {
    ///       "name": "filter",
    ///       "strict": true,
    ///       "schema": {
    ///         "type": "object",
    ///         "properties": {"match": {"type": "boolean"}},
    ///         "required": ["match"],
    ///         "additionalProperties": false
    ///       }
    ///     }
    ///   }
    Poco::JSON::Object::Ptr buildResponseFormat(const ColumnsWithTypeAndName & /*arguments*/) const override
    {
        Poco::JSON::Object::Ptr match_prop = new Poco::JSON::Object;
        match_prop->set("type", "boolean");

        Poco::JSON::Object::Ptr properties = new Poco::JSON::Object;
        properties->set("match", match_prop);

        Poco::JSON::Array::Ptr required = new Poco::JSON::Array;
        required->add("match");

        Poco::JSON::Object::Ptr schema = new Poco::JSON::Object;
        schema->set("type", "object");
        schema->set("properties", properties);
        schema->set("required", required);
        schema->set("additionalProperties", false);

        Poco::JSON::Object::Ptr json_schema = new Poco::JSON::Object;
        json_schema->set("name", "filter");
        json_schema->set("strict", true);
        json_schema->set("schema", schema);

        Poco::JSON::Object::Ptr root = new Poco::JSON::Object;
        root->set("type", "json_schema");
        root->set("json_schema", json_schema);
        return root;
    }

    /// The provider is instructed to return `{"match": <bool>}` via the JSON-schema response format, so in
    /// practice the response parses as an object with a boolean `match` key. Convert it to the canonical
    /// `"true"`/`"false"` string that `parseFilterMatch` expects. Anything else (model ignored the schema,
    /// returned bare text, or returned truncated/invalid JSON) falls through to `raw_response` unchanged, so
    /// `parseFilterMatch`'s fail-closed handling of unrecognised text still applies. Parse errors are
    /// swallowed on purpose - the fail-closed default takes over.
    String postProcessResponse(const String & raw_response) const override
    {
        if (raw_response.empty() || raw_response.front() != '{')
            return raw_response;

        try
        {
            Poco::JSON::Parser parser;
            auto parsed = parser.parse(raw_response);
            auto obj = parsed.extract<Poco::JSON::Object::Ptr>();
            if (obj && obj->has("match"))
                return obj->getValue<bool>("match") ? "true" : "false";
        }
        catch (...) {} // NOLINT(bugprone-empty-catch) Ok: best-effort unwrap, see comment above.

        return raw_response;
    }

    void insertProcessedResult(IColumn & column, const String & processed) const override
    {
        assert_cast<ColumnUInt8 &>(column).insertValue(parseFilterMatch(processed) ? 1 : 0);
    }

    /// Interprets LLM filter output as a boolean. Accepts the text `true` (case- and
    /// whitespace-insensitive). Anything else, including `false` and unrecognised text, maps to
    /// false so a row that the model failed to classify cleanly is filtered out rather than kept.
    static bool parseFilterMatch(std::string_view raw)
    {
        while (!raw.empty() && isWhitespaceASCII(raw.front()))
            raw.remove_prefix(1);
        while (!raw.empty() && isWhitespaceASCII(raw.back()))
            raw.remove_suffix(1);

        if (raw.size() != 4)
            return false;

        String lowered(raw);
        for (char & ch : lowered)
            ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
        return lowered == "true";
    }
};

REGISTER_FUNCTION(AiFilter)
{
    factory.registerFunction<FunctionAiFilter>(FunctionDocumentation{
        .description = R"(
Evaluates a natural-language condition against the given text using an LLM provider and returns a boolean (`UInt8`) suitable for `WHERE`, `PREWHERE`, and `JOIN ... ON`.

The function asks the model to respond with only lowercase `true` or `false`. Any complete response other
than `true` (including `false` and unrecognised text) maps to `0`, so the row is filtered out. A
provider-signalled incomplete reply — truncated, content-filtered, or requiring further action — is instead
treated as an error: with `ai_function_throw_on_error` enabled (the default) the query is aborted; with it
disabled the row maps to `0` and is filtered out.

**Warning:** Do not trust `aiFilter` results without scrutiny. LLM-based predicates can be incorrect
or inconsistent; use them only where false positives and false negatives are acceptable.

Credentials (a named collection specifying the provider, model, endpoint, and optionally an API key)
are taken from the `credentials` key of the optional parameter map, or from the
`ai_function_text_default_credentials` setting when the map omits it.

Note: using `aiFilter` in `JOIN ... ON` evaluates the LLM once per candidate pair and can be expensive.
)",
        .syntax = "aiFilter(text, condition[, params])",
        .arguments = {
            {"text", "Text to evaluate.", {"String"}},
            {"condition", "Constant natural-language condition the text must satisfy.", {"String"}},
            {"params", "Optional constant `Map(String, String)` of parameters. Function-specific keys: `temperature` (sampling temperature controlling randomness; default `0.0`), `max_tokens` (maximum output tokens per call; default `1024`). The common parameters `credentials` and `model` also apply (see [AI Functions](/sql-reference/functions/ai-functions)).", {"Map(String, String)"}},
        },
        .returned_value = {"`1` if the text matches the condition, `0` otherwise. Returns the default value (`0`) if the request failed and `ai_function_throw_on_error` is disabled.", {"UInt8"}},
        .examples = {
            {"Filter angry reviews", "CREATE TABLE reviews (body String) ENGINE = Memory;\nINSERT INTO reviews VALUES ('The package arrived three days late.');\nSELECT * FROM reviews WHERE aiFilter(body, 'the customer is angry about shipping')", ""},
            {"Filter a column with explicit credentials", "CREATE TABLE issues (body String) ENGINE = Memory;\nINSERT INTO issues VALUES ('The application exits unexpectedly after login.');\nSELECT body, aiFilter(body, 'describes a bug', map('credentials', 'ai_text_credentials')) AS is_bug FROM issues LIMIT 5", ""},
        },
        .introduced_in = {26, 8},
        .category = FunctionDocumentation::Category::AI});

    factory.registerAlias("AIFilter", "aiFilter");
}

}
