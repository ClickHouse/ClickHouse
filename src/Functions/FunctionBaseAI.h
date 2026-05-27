#pragma once

#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/AI/IAIProvider.h>
#include <Functions/AI/AIQuotaTracker.h>
#include <DataTypes/DataTypeNullable.h>
#include <Interpreters/Context.h>

namespace DB
{

static constexpr UInt64 DEFAULT_AI_MAX_TOKENS = 1024;

class FunctionBaseAI : public IFunction
{
public:
    explicit FunctionBaseAI(ContextPtr context_);

    bool isStateful() const override { return true; }
    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override { return false; }
    bool isSuitableForConstantFolding() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }
    /// Handle Nullable cols explicitly, since setting this to true may call functions with arbitrary input values
    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return false; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override;

    /// Helpers for nullable col handling

    static bool isStringOrNullableString(const IDataType & type)
    {
        if (isString(type))
            return true;
        if (const auto * nullable = typeid_cast<const DataTypeNullable *>(&type))
            return isString(*nullable->getNestedType());
        return false;
    }

    static DataTypePtr wrapReturnTypeForNullablePrompt(const ColumnsWithTypeAndName & arguments, size_t prompt_idx, DataTypePtr inner)
    {
        if (arguments[prompt_idx].type->isNullable())
            return makeNullable(inner);
        else
            return inner;
    }

    /// Fields read from the named collection that every AI function needs. Function-specific knobs
    /// (max_tokens, temperature, dimensions, …) are layered on top by individual callers.
    struct AINamedCollectionConfig
    {
        String collection_name;
        String provider;
        String endpoint;
        String model;
        String api_key;
        String api_version;
    };

    /// Resolve the named-collection argument: cast the first argument to a `ColumnConst`, run the
    /// `NAMED_COLLECTION` access check, fetch from `NamedCollectionFactory`, and validate that the
    /// required fields (`provider`, `endpoint`, `model`, `api_key`) are non-empty.
    static AINamedCollectionConfig resolveAINamedCollection(const ContextPtr & context, const ColumnPtr & first_arg);

    /// Exponential backoff delay capped at one minute, so adversarial values of
    /// `ai_function_retry_initial_delay_ms` or `ai_function_max_retries` cannot produce a multi-hour
    /// sleep or overflow `std::chrono::milliseconds`.
    static UInt64 computeRetryBackoffMs(UInt64 initial_delay_ms, UInt64 attempt);

protected:
    ContextPtr context;
    ContextPtr getContext() const { return context; }

    virtual String functionName() const = 0;

    /// Temperature controls the randomness or noise of the response. The accepted values depend on the AI provider
    /// (0.0 - 2.0 for OpenAI, 0.0 - 1.0 for Anthropic). Lower is better for more deterministic tasks, while
    /// a higher value is useful for creative tasks, such as chatting or text generation.
    virtual float defaultTemperature() const = 0;

    /// Performs additional validation of the input arguments.
    virtual void checkSanityBeforeExecuteImpl(const ColumnsWithTypeAndName & /*arguments*/, const DataTypePtr & /*result_type*/, size_t /*input_rows_count*/) const {}

    /// A system prompt  applies to each request. AI funcs will probably want to provide a default on a per-function basis.
    virtual String buildSystemPrompt(const ColumnsWithTypeAndName & arguments) const = 0;

    /// The user prompt is appended to the system prompt, this is usually what is contained in each row.
    virtual String buildUserMessage(const ColumnsWithTypeAndName & arguments, size_t row) const = 0;

    /// How the response should be formatted. Different AI providers treat this differently. OpenAI-like providers have a specific field
    /// for the response format, while Anthropic does not but it can be approximated with a tool-use pattern, see AnthropicProvider.h/cpp
    virtual Poco::JSON::Object::Ptr buildResponseFormat(const ColumnsWithTypeAndName & /*arguments*/) const { return nullptr; }

    virtual String postProcessResponse(const String & raw_response) const { return raw_response; }

    /// Index of the per-row text column in the arguments list.
    virtual size_t promptArgumentIndex() const = 0;

    /// Index of the temperature argument. Return 0 if the function doesn't accept temperature.
    virtual size_t temperatureArgumentIndex() const = 0;

private:
    struct ResolvedConfig
    {
        String provider;
        String endpoint;
        String model;
        String api_key;
        String api_version;
        float temperature = 0;
        UInt64 max_tokens = 0;
    };

    ResolvedConfig resolveConfig(const ColumnsWithTypeAndName & arguments) const;
    float resolveTemperature(const ColumnsWithTypeAndName & arguments, const ResolvedConfig & config) const;
};

}
