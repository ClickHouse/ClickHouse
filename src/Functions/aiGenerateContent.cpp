#include <Functions/FunctionBaseAI.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/IDataType.h>
#include <Common/Exception.h>

namespace DB
{

namespace
{

constexpr auto default_system_prompt = "You are a helpful assistant. Provide a clear and concise response.";

class FunctionAiGenerateContent final : public FunctionBaseAI
{
public:
    static constexpr auto name = "aiGenerateContent";

    explicit FunctionAiGenerateContent(ContextPtr context) : FunctionBaseAI(context) {}

    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionAiGenerateContent>(context); }

    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors mandatory_args{
            {"collection", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), &isColumnConst, "const String"},
            {"prompt", nullptr, nullptr, "String or Nullable(String)"},
        };
        FunctionArgumentDescriptors optional_args{
            {"system_prompt", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), &isColumnConst, "const String"},
            {"temperature", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isNumber), &isColumnConst, "const Number"},
        };
        validateFunctionArguments(*this, arguments, mandatory_args, optional_args);

        return std::make_shared<DataTypeString>();
    }

protected:
    String functionName() const override { return name; }

    float defaultTemperature() const override { return 0.7f; }
    size_t promptArgumentIndex() const override { return 1; }
    size_t temperatureArgumentIndex() const override { return 3; }

    String buildSystemPrompt(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() > 2 && isString(arguments[2].type))
        {
            String system_prompt(arguments[2].column->getDataAt(0));
            if (!system_prompt.empty())
                return system_prompt;
        }

        return default_system_prompt;
    }

    String buildUserMessage(const ColumnsWithTypeAndName & arguments, size_t row) const override
    {
        return String(arguments[1].column->getDataAt(row));
    }
};

}

REGISTER_FUNCTION(AiGenerateContent)
{
    factory.registerFunction<FunctionAiGenerateContent>(FunctionDocumentation{
        .description = R"(
Generates free-form text content from a prompt using an LLM provider.

The function sends the prompt to the configured AI provider and returns the generated text.
An optional system prompt can be provided to guide the model's behavior (e.g. tone, format, role).
If no system prompt is given, the default system prompt is: `)" + String(default_system_prompt) + R"(`

The first argument is a named collection that specifies the provider, model, endpoint, and API key.
)",
        .syntax = "aiGenerateContent(collection, prompt[, system_prompt[, temperature]])",
        .arguments
        = {{"collection", "Name of a named collection containing provider credentials and configuration.", {"String"}},
           {"prompt", "The user prompt or question to send to the model.", {"String"}},
           {"system_prompt", "Optional constant system-level instruction that guides the model's behavior (e.g. persona, output format), sent along with each prompt.", {"String"}},
           {"temperature", "Sampling temperature controlling randomness. Default: `0.7`.", {"Float64"}}},
        .returned_value = {"The generated text response, or the default value for the column type (empty string) if the request failed and `ai_function_throw_on_error` is disabled.", {"String"}},
        .examples
        = {{"Simple question", "SELECT aiGenerateContent('ai_credentials', 'What is 2 + 2? Reply with just the number.')", "4"},
           {"With system prompt", "SELECT aiGenerateContent('ai_credentials', 'Explain ClickHouse', 'You are a database expert. Be concise.')", ""},
           {"Summarize column values",
            "SELECT article_title, aiGenerateContent('ai_credentials', concat('Summarize in one sentence: ', article_body)) AS summary FROM articles LIMIT 5",
            ""}},
        .introduced_in = {26, 4},
        .category = FunctionDocumentation::Category::AI});
}

}
