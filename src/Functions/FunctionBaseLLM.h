#pragma once

#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/LLM/ILLMProvider.h>
#include <Functions/LLM/LLMResultCache.h>
#include <Functions/LLM/LLMQuotaTracker.h>
#include <Interpreters/Context.h>

namespace DB
{

class FunctionBaseLLM : public IFunction
{
public:
    explicit FunctionBaseLLM(ContextPtr context);

    bool isStateful() const override { return true; }
    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return false; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return false; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override;

protected:
    virtual String functionName() const = 0;
    virtual float defaultTemperature() const = 0;
    virtual String buildSystemPrompt(const ColumnsWithTypeAndName & arguments) const = 0;
    virtual String buildUserMessage(const ColumnsWithTypeAndName & arguments, size_t row) const = 0;
    virtual String buildResponseFormatJSON(const ColumnsWithTypeAndName & arguments) const { (void)arguments; return ""; }
    virtual String postProcessResponse(const String & raw_response) const { return raw_response; }

    virtual size_t getTextColumnIndex() const { return 0; }
    virtual bool hasNamedCollectionArg(const ColumnsWithTypeAndName & arguments) const;
    size_t getFirstDataArgIndex(const ColumnsWithTypeAndName & arguments) const;


private:
    struct ResolvedConfig
    {
        String provider;
        String endpoint;
        String model;
        String api_key;
        float temperature;
        UInt64 max_tokens;
    };

    ResolvedConfig resolveConfig(const ColumnsWithTypeAndName & arguments) const;
    float resolveTemperature(const ColumnsWithTypeAndName & arguments, const ResolvedConfig & config) const;

    const UInt64 timeout_sec;
    const UInt64 max_concurrent;
    const UInt64 max_rps;
    const UInt64 max_retries;
    const UInt64 retry_delay_ms;
    const UInt64 cache_ttl;

    const String default_llm_resource;

    const UInt64 llm_max_rows_per_query;
    const UInt64 llm_max_input_tokens_per_query;
    const UInt64 llm_max_output_tokens_per_query;
    const UInt64 llm_max_api_calls_per_query;
    const String llm_on_quota_exceeded;
    const String llm_on_error;

    const Settings & settings;
    const ServerSettings & server_settings;
};

}
