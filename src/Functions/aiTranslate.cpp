#include <Functions/FunctionBaseAI.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypeString.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

class FunctionAiTranslate final : public FunctionBaseAI
{
public:
    static constexpr auto name = "aiTranslate";

    explicit FunctionAiTranslate(ContextPtr context_) : FunctionBaseAI(context_) {}

    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionAiTranslate>(context_); }

    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors mandatory_args{
            {"text", static_cast<FunctionArgumentDescriptor::TypeValidator>(&FunctionBaseAI::isStringOrNullableString), nullptr, "String or Nullable(String)"},
            {"target_language", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), &isColumnConst, "const String"},
        };
        FunctionArgumentDescriptors optional_args{
            {"params", static_cast<FunctionArgumentDescriptor::TypeValidator>(&FunctionBaseAI::isStringToStringMap), &isColumnConst, "const Map(String, String)"},
        };
        validateFunctionArguments(*this, arguments, mandatory_args, optional_args);

        return wrapReturnTypeForNullablePrompt(arguments, 0, std::make_shared<DataTypeString>());
    }

private:
    static constexpr float default_temp = 0.3f;
    static constexpr size_t target_language_arg_index = 1;

    String functionName() const override { return name; }

    AIParamSpecs functionParams() const override
    {
        return {
            {"temperature", AIParamKind::Float, Field(static_cast<Float64>(default_temp))},
            {"instructions", AIParamKind::String, Field(String(""))},
        };
    }

    void checkSanityBeforeExecuteImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t /*input_rows_count*/) const override
    {
        auto target_language = arguments[target_language_arg_index].column->getDataAt(0);
        if (target_language.find_first_not_of(" \t\n\r") == std::string_view::npos)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "aiTranslate: 'target_language' must not be empty");
    }

    String buildSystemPrompt(const ColumnsWithTypeAndName & arguments, const AIParams & params) const override
    {
        auto target_language = String(arguments[target_language_arg_index].column->getDataAt(0));
        auto prompt = "Translate the following text into " + target_language + ". Return only the translation, nothing else.";

        auto instructions = params.getString("instructions");
        if (!instructions.empty())
            prompt += " Additional instructions: " + instructions;
        return prompt;
    }

    String buildUserMessage(const ColumnsWithTypeAndName & arguments, size_t row) const override
    {
        return String(arguments[0].column->getDataAt(row));
    }
};

REGISTER_FUNCTION(AiTranslate)
{
    factory.registerFunction<FunctionAiTranslate>(FunctionDocumentation{
        .description = R"(
Translates the given text into the specified target language using an LLM provider.

Additional style or dialect instructions may be passed via the `instructions` key of the parameter map (e.g. `'keep technical terms untranslated'`).

Credentials (a named collection specifying the provider, model, endpoint, and optionally an API key)
are taken from the `credentials` key of the optional parameter map, or from the
`ai_function_text_default_credentials` setting when the map omits it.
)",
        .syntax = "aiTranslate(text, target_language[, params])",
        .arguments = {
            {"text", "Text to translate.", {"String"}},
            {"target_language", "Target language name or BCP-47 code (e.g. `'French'`, `'es-MX'`).", {"String"}},
            {"params", "Optional constant `Map(String, String)` of parameters. Function-specific keys: `temperature` (sampling temperature controlling randomness; default `0.3`), `max_tokens` (maximum output tokens per call; default `1024`), `instructions` (additional style or dialect instructions for the translator). The common parameters `credentials` and `model` also apply (see [AI Functions](/sql-reference/functions/ai-functions)).", {"Map(String, String)"}},
        },
        .returned_value = {"The translated text, or the default value for the column type (empty string) if the request failed and `ai_function_throw_on_error` is disabled.", {"String"}},
        .examples = {
            {"Translate to French", "SELECT aiTranslate('Hello, world!', 'French')", "Bonjour le monde!"},
            {"Translate to Japanese with style instructions", "SELECT aiTranslate(body, 'Japanese', map('instructions', 'Use polite form (desu/masu)')) FROM articles LIMIT 5", ""},
        },
        .introduced_in = {26, 4},
        .category = FunctionDocumentation::Category::AI});

    factory.registerAlias("AITranslate", "aiTranslate");
}

}
