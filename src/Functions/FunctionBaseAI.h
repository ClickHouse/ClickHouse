#pragma once

#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/AI/IAIProvider.h>
#include <Functions/AI/AIQuotaTracker.h>
#include <DataTypes/DataTypeNullable.h>
#include <Interpreters/Context.h>
#include <Core/Field.h>

#include <exception>
#include <functional>
#include <map>
#include <optional>
#include <string_view>
#include <vector>

namespace DB
{

static constexpr UInt64 DEFAULT_AI_MAX_TOKENS = 1024;

/// Logical type of an AI-function parameter. The parameter map is `Map(String, String)`, so every
/// value arrives as a `String`; the kind tells the resolver how to parse each one into its real type
/// (and how to read the same key from a named collection).
enum class AIParamKind
{
    String,
    Float,
    UInt,
};

/// Declarative description of one parameter an AI function accepts in its trailing
/// `Map(String, String)` argument. Each function declares its own params; `FunctionBaseAI`
/// contributes the common ones (`credentials`, `model`, `max_tokens`).
struct AIParamSpec
{
    std::string_view name;
    AIParamKind kind;
    /// `std::nullopt` => required: the resolver throws if the key is absent everywhere.
    std::optional<Field> default_value;
    /// When true, a missing map key falls back to the same-named field of the named collection
    /// before falling back to `default_value` (e.g. `model`, `max_tokens`).
    bool inherit_from_collection = false;
};

/// Small, short-lived config containers (a handful of entries, not user-data-scaled), so the
/// default allocator is fine and a memory-tracking variant would add nothing.
using AIParamSpecs = std::vector<AIParamSpec>; // STYLE_CHECK_ALLOW_STD_CONTAINERS
using AIParamValues = std::map<String, Field, std::less<>>; // STYLE_CHECK_ALLOW_STD_CONTAINERS

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

    /// Connection fields read from the named collection. `model` is resolved separately as a
    /// parameter (it may be overridden via the map), so it is not part of the connection config.
    struct AINamedCollectionConfig
    {
        String collection_name;
        String provider;
        String endpoint;
        String api_key;
        String api_version;
    };

    /// Resolved parameters for one AI-function call: the named-collection connection config plus the
    /// per-parameter values (map override -> named collection -> declared default). Only keys that
    /// resolved to a value are present; `has` distinguishes "absent" from "set to empty string".
    struct AIParams
    {
        AINamedCollectionConfig collection;
        AIParamValues values;

        bool has(std::string_view key) const { return values.contains(key); }

        String getString(std::string_view key) const;
        Float64 getFloat(std::string_view key) const;
        UInt64 getUInt(std::string_view key) const;
    };

    /// Type validator for the optional trailing parameter argument: a `Map(String, String)`.
    static bool isStringToStringMap(const IDataType & type);

    /// Resolve the trailing `Map(String, String)` argument (if any) against `spec`:
    ///  - reject any map key not declared in `spec`;
    ///  - resolve `credentials` (map key -> `default_credentials` -> throw), then load and
    ///    access-check the named collection and validate `provider`/`endpoint`;
    ///  - resolve every other spec entry: map override -> (if `inherit_from_collection`) named
    ///    collection field -> `default_value` -> throw when required and absent.
    static AIParams resolveAIParams(
        const ContextPtr & context,
        const ColumnsWithTypeAndName & arguments,
        const AIParamSpecs & spec,
        const String & default_credentials);

    /// Parameters common to every AI function. Function-specific params are appended by `functionParams`.
    static AIParamSpecs commonParams();

    /// Exponential backoff delay capped at one minute, so adversarial values of
    /// `ai_function_retry_initial_delay_ms` or `ai_function_max_retries` cannot produce a multi-hour
    /// sleep or overflow `std::chrono::milliseconds`.
    static UInt64 computeRetryBackoffMs(UInt64 initial_delay_ms, UInt64 attempt);

    /// Whether a failed provider request should be retried: transient network failures and
    /// transient/server-side HTTP responses are retriable, deterministic argument/usage errors are not.
    static bool isRetriableProviderError(std::exception_ptr exception);

protected:
    ContextPtr context;
    ContextPtr getContext() const { return context; }

    virtual String functionName() const = 0;

    /// Function-specific parameters accepted in the trailing `Map(String, String)` argument, on top
    /// of `commonParams`. Each entry carries its own default (or is required). Default: none.
    virtual AIParamSpecs functionParams() const { return {}; }

    /// Performs additional validation of the input arguments.
    virtual void checkSanityBeforeExecuteImpl(const ColumnsWithTypeAndName & /*arguments*/, const DataTypePtr & /*result_type*/, size_t /*input_rows_count*/) const {}

    /// A system prompt  applies to each request. AI funcs will probably want to provide a default on a per-function basis.
    virtual String buildSystemPrompt(const ColumnsWithTypeAndName & arguments, const AIParams & params) const = 0;

    /// The user prompt is appended to the system prompt, this is usually what is contained in each row.
    virtual String buildUserMessage(const ColumnsWithTypeAndName & arguments, size_t row) const = 0;

    /// How the response should be formatted. Different AI providers treat this differently. OpenAI-like providers have a specific field
    /// for the response format, while Anthropic does not but it can be approximated with a tool-use pattern, see AnthropicProvider.h/cpp
    virtual Poco::JSON::Object::Ptr buildResponseFormat(const ColumnsWithTypeAndName & /*arguments*/) const { return nullptr; }

    virtual String postProcessResponse(const String & raw_response) const { return raw_response; }

private:
    /// Full parameter spec for this function: `commonParams` followed by `functionParams`.
    AIParamSpecs allParams() const;
};

}
