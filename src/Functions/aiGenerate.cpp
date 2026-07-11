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

class FunctionAiGenerate final : public FunctionBaseAI
{
public:
    static constexpr auto name = "aiGenerate";

    explicit FunctionAiGenerate(ContextPtr context_) : FunctionBaseAI(context_) {}

    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionAiGenerate>(context_); }

    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors mandatory_args{
            {"prompt", static_cast<FunctionArgumentDescriptor::TypeValidator>(&FunctionBaseAI::isStringOrNullableString), nullptr, "String or Nullable(String)"},
        };
        FunctionArgumentDescriptors optional_args{
            {"params", static_cast<FunctionArgumentDescriptor::TypeValidator>(&FunctionBaseAI::isStringToStringMap), &isColumnConst, "const Map(String, String)"},
        };
        validateFunctionArguments(*this, arguments, mandatory_args, optional_args);

        return wrapReturnTypeForNullablePrompt(arguments, 0, std::make_shared<DataTypeString>());
    }

private:
    static constexpr float default_temp = 0.7f;

    String functionName() const override { return name; }

    AIParamSpecs functionParams() const override
    {
        return {
            {"temperature", AIParamKind::Float, Field(static_cast<Float64>(default_temp))},
            {"system_prompt", AIParamKind::String, Field(String(default_system_prompt))},
        };
    }

    String buildSystemPrompt(const ColumnsWithTypeAndName &, const AIParams & params) const override
    {
        return params.getString("system_prompt");
    }

    String buildUserMessage(const ColumnsWithTypeAndName & arguments, size_t row) const override
    {
        return String(arguments[0].column->getDataAt(row));
    }
};

}

REGISTER_FUNCTION(AiGenerate)
{
    factory.registerFunction<FunctionAiGenerate>(FunctionDocumentation{
        .description = R"(
Generates free-form text content from a prompt using an LLM provider.

The function sends the prompt to the configured AI provider and returns the generated text.

Credentials (a named collection specifying the provider, model, endpoint, and optionally an API key)
are taken from the `credentials` key of the optional parameter map, or from the
`ai_function_text_default_credentials` setting when the map omits it.

The optional parameter map may also set `system_prompt` (an instruction that guides the model's
behavior, e.g. tone, format, role), `temperature`, `max_tokens`, and `model`. If `system_prompt` is
not set, the default is: `)" + String(default_system_prompt) + R"(`
)",
        .syntax = "aiGenerate(prompt[, params])",
        .arguments
        = {{"prompt", "The user prompt or question to send to the model.", {"String"}},
           {"params", "Optional constant `Map(String, String)` of parameters. Function-specific keys: `temperature` (sampling temperature controlling randomness; default `0.7`), `max_tokens` (maximum output tokens per call; default `1024`), `system_prompt` (constant system-level instruction guiding the model's behavior; default a generic assistant prompt). The common parameters `credentials` and `model` also apply (see [AI Functions](/sql-reference/functions/ai-functions)).", {"Map(String, String)"}}},
        .returned_value = {"The generated text response, or the default value for the column type (empty string) if the request failed and `ai_function_throw_on_error` is disabled.", {"String"}},
        .examples
        = {{"Simple question", "SELECT aiGenerate('What is 2 + 2? Reply with just the number.')", "4"},
           {"With explicit credentials and system prompt", "SELECT aiGenerate('Explain ClickHouse', map('credentials', 'ai_text_credentials', 'system_prompt', 'You are a database expert. Be concise.'))", ""},
           {"Summarize column values", "SELECT article_title, aiGenerate(concat('Summarize in one sentence: ', article_body)) AS summary FROM articles LIMIT 5", ""}},
        .introduced_in = {26, 4},
        .category = FunctionDocumentation::Category::AI});

        factory.registerAlias("AIGenerate", "aiGenerate");
}


}
