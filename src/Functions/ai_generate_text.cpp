#include "config.h"

#if USE_CLIENT_AI

#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Common/Exception.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <Interpreters/AI/AITextGenerator.h>
#include <Interpreters/AI/AIRequestOptions.h>


namespace DB
{

namespace Setting
{
    extern const SettingsBool allow_experimental_ai_functions;
}

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int SUPPORT_IS_DISABLED;
    extern const int ILLEGAL_COLUMN;
}

class FunctionAIGenerateText : public IFunction, WithContext
{
public:
    static constexpr auto name = "ai_generate_text";

    explicit FunctionAIGenerateText(ContextPtr context_)
        : WithContext(context_)
    {
        if (!getContext()->getSettingsRef()[Setting::allow_experimental_ai_functions])
            throw Exception(
                ErrorCodes::SUPPORT_IS_DISABLED,
                "Function `{}` is experimental. Set `allow_experimental_ai_functions` to enable it",
                name);
    }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    /// Allow short-circuit evaluation to avoid expensive AI calls for filtered rows
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }

    /// system_prompt (arg 0) and options_json (arg 2, if present) must be constant
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {0, 2}; }

    bool useDefaultImplementationForConstants() const override { return false; }
    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override { return false; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() < 2 || arguments.size() > 3)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} expects 2 or 3 arguments: system_prompt, user_prompt[, options_json]. Got {}",
                getName(), arguments.size());

        for (size_t i = 0; i < arguments.size(); ++i)
        {
            if (!isString(arguments[i].type))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type {} of argument {} of function {}. Must be String",
                    arguments[i].type->getName(), i + 1, getName());
        }

        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(
        const ColumnsWithTypeAndName & arguments,
        const DataTypePtr &,
        size_t input_rows_count) const override
    {
        /// 1. Parse arguments
        auto system_prompt = getConstStringArgument(arguments, 0, "system_prompt");

        auto user_prompt_col = arguments[1].column->convertToFullColumnIfConst();
        const auto * user_prompt_column = checkAndGetColumn<ColumnString>(user_prompt_col.get());
        if (!user_prompt_column)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Second argument of function {} must be a String column",
                getName());

        String options_json;
        if (arguments.size() == 3)
            options_json = getConstStringArgument(arguments, 2, "options_json");

        /// 2. Parse and validate options
        auto options = AIRequestOptions::fromJSON(options_json);
        options.mergeWithConfig(getContext()->getConfigRef());
        options.validate();

        /// 3. Generate text using AITextGenerator
        AITextGenerator generator(getContext());
        return generator.generateText(system_prompt, *user_prompt_column, options, input_rows_count);
    }

private:
    static String getConstStringArgument(
        const ColumnsWithTypeAndName & arguments,
        size_t index,
        const char * arg_name)
    {
        const auto * col_const = checkAndGetColumnConst<ColumnString>(arguments[index].column.get());
        if (!col_const)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Argument '{}' (position {}) of function {} must be a constant String",
                arg_name, index + 1, name);
        return col_const->getValue<String>();
    }
};


REGISTER_FUNCTION(AIGenerateText)
{
    FunctionDocumentation::Description description = R"(
Calls an LLM API to generate text based on a prompt.
Supports OpenAI and Anthropic providers via the ai-sdk-cpp library.
Requires the `allow_experimental_ai_functions` setting to be enabled.

All parameters can be configured in the server configuration under the `ai` section:
`ai.provider`, `ai.model`, `ai.api_key`, `ai.base_url`, `ai.temperature`, `ai.max_tokens`,
`ai.top_p`, `ai.seed`, `ai.frequency_penalty`, `ai.presence_penalty`, `ai.requests_per_minute`,
`ai.max_retries`, `ai.retry_delay_ms`, `ai.retry_max_delay_ms`.
Values specified in `options_json` take priority over the server configuration.
When `api_key` is not set in the configuration, it falls back to environment variables
(`OPENAI_API_KEY` / `ANTHROPIC_API_KEY`). When `base_url` is not set, the provider's
default is used (`https://api.openai.com` for OpenAI, `https://api.anthropic.com` for Anthropic).

When `system_prompt` is an empty string, a default system prompt is automatically used that describes the assistant as a ClickHouse semantic analysis expert. To completely omit the system prompt, you must explicitly pass a non-empty value. `options_json` is optional.

Supported fields in `options_json` (all optional, valid ranges depend on the provider):
- `provider` (String) — AI provider name: 'openai' or 'anthropic'. Overrides `ai.provider` in config.xml.
- `model` (String) — Model identifier, e.g. 'gpt-4o' or 'claude-sonnet-4-5'. Overrides `ai.model` in config.xml.
- `api_key` (String) — API key for authentication. Overrides `ai.api_key` in config.xml and environment variables.
- `base_url` (String) — Base URL for the API endpoint. Overrides `ai.base_url` in config.xml and provider defaults.
- `temperature` (Float64) — Controls randomness of the output. Lower values make output more deterministic, higher values more creative. Overrides `ai.temperature` in config.xml.
- `max_tokens` (Int) — Maximum number of tokens to generate in the response. Overrides `ai.max_tokens` in config.xml.
- `top_p` (Float64) — Nucleus sampling: only tokens with cumulative probability up to `top_p` are considered. An alternative to `temperature`. Overrides `ai.top_p` in config.xml.
- `seed` (Int) — Random seed for reproducible generation (best-effort, not guaranteed by all providers). Overrides `ai.seed` in config.xml.
- `frequency_penalty` (Float64) — Penalizes tokens based on how often they appear in the output so far. Positive values reduce repetition. Overrides `ai.frequency_penalty` in config.xml.
- `presence_penalty` (Float64) — Penalizes tokens based on whether they have appeared in the output at all. Positive values encourage topic diversity. Overrides `ai.presence_penalty` in config.xml.
- `requests_per_minute` (Int, default 0) — Maximum number of API calls per minute. 0 means no rate limiting. Useful to stay within provider rate limits across a large query. Overrides `ai.requests_per_minute` in config.xml.
- `max_retries` (Int, default 3, max 20) — Maximum number of automatic retries when a rate-limit error is returned by the API. Overrides `ai.max_retries` in config.xml.
- `retry_delay_ms` (Int, default 1000, max 3600000) — Initial retry delay in milliseconds. Doubles on each subsequent retry (exponential backoff). Overrides `ai.retry_delay_ms` in config.xml.
- `retry_max_delay_ms` (Int, default 60000) — Maximum retry delay in milliseconds. Caps the exponential backoff so the delay never exceeds this value. Must be >= `retry_delay_ms`. Overrides `ai.retry_max_delay_ms` in config.xml.
)";
    FunctionDocumentation::Syntax syntax
        = "ai_generate_text(system_prompt, user_prompt [, options_json])";
    FunctionDocumentation::Arguments arguments = {
        {"system_prompt", "System prompt for the LLM. Required; pass an empty string `''` to use the default system prompt (a ClickHouse semantic analysis expert). To completely omit the system prompt, pass a space `' '` or other non-empty value.", {"String"}},
        {"user_prompt", "The user prompt to send to the LLM. Required; can reference a table column.", {"String"}},
        {"options_json",
         "Optional JSON object with generation parameters and provider/model settings. "
         "Can be omitted entirely; individual fields default to: "
         "`provider` — falls back to `ai.provider` in config.xml (required if not set there); "
         "`model` — falls back to `ai.model` in config.xml (required if not set there); "
         "`temperature`, `max_tokens`, `top_p`, `seed`, `frequency_penalty`, `presence_penalty` — fall back to `ai.*` in config.xml, then provider defaults; "
         "`requests_per_minute` — falls back to `ai.requests_per_minute` in config.xml, then 0 (no limit); `max_retries` — falls back to `ai.max_retries` in config.xml, then 3 (max 20); `retry_delay_ms` — falls back to `ai.retry_delay_ms` in config.xml, then 1000 (max 3600000); `retry_max_delay_ms` — falls back to `ai.retry_max_delay_ms` in config.xml, then 60000.",
         {"String"}},
    };
    FunctionDocumentation::ReturnedValue returned_value
        = {"The generated text from the LLM.", {"String"}};
    FunctionDocumentation::Examples examples = {
        {"Basic usage with default system prompt (provider and model from config.xml)",
         "SELECT ai_generate_text('', 'What is ClickHouse?')",
         "ClickHouse is a column-oriented database management system..."},
        {"With custom system prompt",
         "SELECT ai_generate_text('Answer in one sentence', 'What is ClickHouse?', "
         "'{\"provider\": \"openai\", \"model\": \"gpt-4o\"}')",
         "ClickHouse is a fast open-source columnar database for real-time analytics."},
        {"With generation options",
         "SELECT ai_generate_text('Be creative', 'Write a haiku about databases', "
         "'{\"provider\": \"openai\", \"model\": \"gpt-4o\", \"temperature\": 0.9, \"max_tokens\": 100}')",
         "Rows align in columns..."},
        {"With rate limiting and retry (useful for services with per-minute request limits)",
         "SELECT ai_generate_text('Summarize in one sentence', text, "
         "'{\"provider\": \"openai\", \"model\": \"gpt-4o\", \"requests_per_minute\": 18, \"max_retries\": 5}') "
         "FROM my_table",
         ""},
    };
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation::IntroducedIn introduced_in = {26, 3};
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction(
        "ai_generate_text",
        [](ContextPtr context) -> FunctionPtr
        {
            return std::make_shared<FunctionAIGenerateText>(context);
        },
        documentation);
}

}

#endif
