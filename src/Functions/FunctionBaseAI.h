#pragma once

#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/AI/IAIProvider.h>
#include <Functions/AI/AIResultCache.h>
#include <Functions/AI/AIQuotaTracker.h>
#include <Interpreters/Context.h>

namespace DB
{

static constexpr auto DEFAULT_AI_PROVIDER = "openai";
static constexpr UInt64 DEFAULT_AI_MAX_TOKENS = 1024;

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
    virtual float defaultTemperature() const = 0;
    virtual String buildSystemPrompt(const ColumnsWithTypeAndName & arguments) const = 0;
    virtual String buildUserMessage(const ColumnsWithTypeAndName & arguments, size_t row) const = 0;
    virtual String buildResponseFormatJSON(const ColumnsWithTypeAndName & /*arguments*/) const { return ""; }
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
};

}
