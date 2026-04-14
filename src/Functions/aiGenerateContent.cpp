#include <Functions/FunctionBaseAI.h>
#include <Functions/FunctionFactory.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/IDataType.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

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
        if (arguments.size() < 2 || arguments.size() > 4)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} requires 2-4 arguments: collection, prompt[, system_prompt[, temperature]]", name);

        /// Temperature (float) requires system_prompt (string) before it
        if (arguments.size() > FIRST_DATA_ARG_INDEX + 1 && isFloat(arguments[FIRST_DATA_ARG_INDEX + 1].type))
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} requires a system_prompt argument (String) before temperature (Float)", name);

        return std::make_shared<DataTypeString>();
    }

protected:
    String functionName() const override { return name; }

    /// Higher temp for some creativity due to nature of the task.
    float defaultTemperature() const override { return 0.7f; }

    String buildSystemPrompt(const ColumnsWithTypeAndName & arguments) const override
    {
        size_t prompt_idx = FIRST_DATA_ARG_INDEX;

        if (arguments.size() > prompt_idx + 1 && isString(arguments[prompt_idx + 1].type))
        {
            String system_prompt(arguments[prompt_idx + 1].column->getDataAt(0));
            if (!system_prompt.empty())
                return system_prompt;
        }

        return default_system_prompt;
    }

    String buildUserMessage(const ColumnsWithTypeAndName & arguments, size_t row) const override
    {
        size_t prompt_idx = FIRST_DATA_ARG_INDEX;
        return String(arguments[prompt_idx].column->getDataAt(row));
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
           {"system_prompt", "Optional system-level instruction that guides the model's behavior (e.g. persona, output format).", {"String"}},
           {"temperature", "Sampling temperature controlling randomness. Default: `0.7`.", {"Float64"}}},
        .returned_value = {"The generated text response, or the default value for the column type (empty string) if the request failed and `ai_on_error` is set to `'default'`.", {"String"}},
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
