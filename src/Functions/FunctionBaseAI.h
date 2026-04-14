#pragma once

#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/AI/IAIProvider.h>
#include <Functions/AI/AIQuotaTracker.h>
#include <Interpreters/Context.h>

namespace DB
{

static constexpr auto DEFAULT_AI_PROVIDER = "openai";
static constexpr UInt64 DEFAULT_AI_MAX_TOKENS = 1024;
static constexpr size_t FIRST_DATA_ARG_INDEX = 1;

class FunctionBaseAI : public IFunction
{
public:
    explicit FunctionBaseAI(ContextPtr context_);

    bool isStateful() const override { return true; }
    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return false; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override;

protected:
    ContextWeakPtr context_weak;
    ContextPtr getContext() const { return context_weak.lock(); }

    virtual String functionName() const = 0;

    /// Temperature controls the randomness or noise of the response. The accepted values depend on the AI provider
    /// (0.0 - 2.0 for OpenAI, 0.0 - 1.0 for Anthropic). Lower is better for more deterministic tasks, while
    /// a higher value is useful for creative tasks, such as chatting or text generation.
    virtual float defaultTemperature() const = 0;

    /// A system prompt  applies to each request. AI funcs will probably want to provide a default on a per-function basis.
    virtual String buildSystemPrompt(const ColumnsWithTypeAndName & arguments) const = 0;

    /// The user prompt is appended to the system prompt, this is usually what is contained in each row.
    virtual String buildUserMessage(const ColumnsWithTypeAndName & arguments, size_t row) const = 0;

    /// How the response should be formatted. Different AI providers treat this differently. OpenAI-like providers have a specific field
    /// for the response format, while Anthropic does not but it can be approximated with a tool-use pattern, see AnthropicProvider.h/cpp
    virtual Poco::JSON::Object::Ptr buildResponseFormat(const ColumnsWithTypeAndName & /*arguments*/) const { return nullptr; }

    virtual String postProcessResponse(const String & raw_response) const { return raw_response; }



private:
    struct ResolvedConfig
    {
        String provider;
        String endpoint;
        String model;
        String api_key;
        String api_version;
        float temperature;
        UInt64 max_tokens;
    };

    ResolvedConfig resolveConfig(const ColumnsWithTypeAndName & arguments) const;
    float resolveTemperature(const ColumnsWithTypeAndName & arguments, const ResolvedConfig & config) const;
};

}
