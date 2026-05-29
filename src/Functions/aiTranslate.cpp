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

    explicit FunctionAiTranslate(ContextPtr context) : FunctionBaseAI(context) {}

    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionAiTranslate>(context); }

    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors mandatory_args{
            {"collection", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), &isColumnConst, "const String"},
            {"text", static_cast<FunctionArgumentDescriptor::TypeValidator>(&FunctionBaseAI::isStringOrNullableString), nullptr, "String or Nullable(String)"},
            {"target_language", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), &isColumnConst, "const String"},
        };
        FunctionArgumentDescriptors optional_args{
            {"instructions", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), &isColumnConst, "const String"},
            {"temperature", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isNumber), &isColumnConst, "const Number"},
        };
        validateFunctionArguments(*this, arguments, mandatory_args, optional_args);

        return wrapReturnTypeForNullablePrompt(arguments, prompt_arg_index, std::make_shared<DataTypeString>());
    }

private:
    static constexpr float default_temp = 0.3f;
    static constexpr size_t prompt_arg_index = 1;
    static constexpr size_t target_language_arg_index = 2;
    static constexpr size_t instructions_arg_index = 3;
    static constexpr size_t temp_arg_idx = 4;

    String functionName() const override { return name; }

    float defaultTemperature() const override { return default_temp; }
    size_t promptArgumentIndex() const override { return prompt_arg_index; }
    size_t temperatureArgumentIndex() const override { return temp_arg_idx; }

    void checkSanityBeforeExecuteImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
    {
        if (input_rows_count)
        {
            auto target_language = arguments[target_language_arg_index].column->getDataAt(0);
            if (target_language.find_first_not_of(" \t\n\r") == std::string_view::npos)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "aiTranslate: 'target_language' must not be empty");
        }
    }

    String buildSystemPrompt(const ColumnsWithTypeAndName & arguments) const override
    {
        auto target_language = String(arguments[target_language_arg_index].column->getDataAt(0));
        auto prompt = "Translate the following text into " + target_language + ". Return only the translation, nothing else.";

        if (arguments.size() > instructions_arg_index)
        {
            auto instructions = String(arguments[instructions_arg_index].column->getDataAt(0));
            if (!instructions.empty())
                prompt += " Additional instructions: " + instructions;
        }
        return prompt;
    }

    String buildUserMessage(const ColumnsWithTypeAndName & arguments, size_t row) const override
    {
        return String(arguments[prompt_arg_index].column->getDataAt(row));
    }
};

REGISTER_FUNCTION(AiTranslate)
{
    factory.registerFunction<FunctionAiTranslate>(FunctionDocumentation{
        .description = R"(
Translates the given text into the specified target language using an LLM provider.

Additional style or dialect instructions may be passed as a fourth argument (e.g. `'keep technical terms untranslated'`).

The first argument is a named collection that specifies the provider, model, endpoint, and API key.
)",
        .syntax = "aiTranslate(collection, text, target_language[, instructions[, temperature]])",
        .arguments = {
            {"collection", "Name of a named collection containing provider credentials and configuration.", {"String"}},
            {"text", "Text to translate.", {"String"}},
            {"target_language", "Target language name or BCP-47 code (e.g. `'French'`, `'es-MX'`).", {"String"}},
            {"instructions", "Optional constant additional instructions for the translator.", {"String"}},
            {"temperature", "Sampling temperature controlling randomness. Default: `0.3`.", {"Float64"}},
        },
        .returned_value = {"The translated text, or the default value for the column type (empty string) if the request failed and `ai_function_throw_on_error` is disabled.", {"String"}},
        .examples = {
            {"Translate to French", "SELECT aiTranslate('ai_credentials', 'Hello, world!', 'French')", "Bonjour le monde!"},
            {"Translate to Japanese with style instructions", "SELECT aiTranslate('ai_credentials', body, 'Japanese', 'Use polite form (desu/masu)') FROM articles LIMIT 5", ""},
        },
        .introduced_in = {26, 4},
        .category = FunctionDocumentation::Category::AI});

    factory.registerAlias("AITranslate", "aiTranslate");
}

}
