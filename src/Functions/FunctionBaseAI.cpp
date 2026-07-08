#include <Functions/FunctionBaseAI.h>
#include <Access/Common/AccessType.h>
#include <Access/ContextAccess.h>
#include <Common/ProfileEvents.h>
#include <Common/Exception.h>
#include <Common/NetException.h>
#include <Poco/Net/NetException.h>
#include <algorithm>
#include <exception>
#include <thread>
#include <utility>
#include <Common/logger_useful.h>
#include <Common/NamedCollections/NamedCollectionsFactory.h>
#include <Common/RemoteHostFilter.h>
#include <Poco/URI.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeMap.h>
#include <IO/ConnectionTimeouts.h>
#include <IO/HTTPCommon.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromString.h>
#include <Core/Settings.h>
#include <Core/ServerSettings.h>
#include <limits>

namespace ProfileEvents
{
    extern const Event AIInputTokens;
    extern const Event AIOutputTokens;
    extern const Event AIAPICalls;
    extern const Event AIRowsProcessed;
    extern const Event AIRowsSkipped;
}

namespace DB
{

namespace Setting
{
    extern const SettingsBool allow_experimental_ai_functions;
    extern const SettingsUInt64 ai_function_request_timeout_sec;
    extern const SettingsUInt64 ai_function_max_retries;
    extern const SettingsUInt64 ai_function_retry_initial_delay_ms;
    extern const SettingsBool ai_function_throw_on_error;
    extern const SettingsUInt64 ai_function_max_input_tokens_per_query;
    extern const SettingsUInt64 ai_function_max_output_tokens_per_query;
    extern const SettingsUInt64 ai_function_max_api_calls_per_query;
    extern const SettingsBool ai_function_throw_on_quota_exceeded;
    extern const SettingsString ai_function_text_default_credentials;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int SUPPORT_IS_DISABLED;
}

namespace
{

/// Strip control characters (U+0000..U+001F except \t \n \r) that break JSON serialization.
String sanitizeTextForAI(std::string_view input)
{
    String output;
    output.reserve(input.size());
    for (unsigned char ch : input)
    {
        if (ch < 0x20 && ch != '\t' && ch != '\n' && ch != '\r')
            output.push_back(' ');
        else
            output.push_back(static_cast<char>(ch));
    }
    return output;
}

/// Providers serialize integer fields (`max_tokens`, `dimensions`) as `Int64` (Poco JSON has no
/// UInt64). Reject values that would silently become negative after the cast.
void checkUIntFitsInt64(UInt64 value, std::string_view name)
{
    if (value > static_cast<UInt64>(std::numeric_limits<Int64>::max()))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "AI function parameter '{}' exceeds maximum ({})",
            name, std::numeric_limits<Int64>::max());
}

/// Parse a map value (string) into a `Field` of the parameter's kind.
Field parseAIParamValue(AIParamKind kind, const String & raw, std::string_view name)
{
    switch (kind)
    {
        case AIParamKind::String:
            return Field(raw);
        case AIParamKind::Float:
            try
            {
                return Field(parseFromString<Float64>(raw));
            }
            catch (...)
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "AI function parameter '{}' must be a number, got '{}'", name, raw);
            }
        case AIParamKind::UInt:
        {
            /// Special UInt64 handling to avoid potential overflow
            UInt64 value = 0;
            ReadBufferFromString buf(raw);
            if (!tryReadIntText<ReadIntTextCheckOverflow::CHECK_OVERFLOW>(value, buf) || !buf.eof())
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "AI function parameter '{}' must be a non-negative integer, got '{}'", name, raw);
            checkUIntFitsInt64(value, name);
            return Field(value);
        }
    }
    std::unreachable();
}

/// Read a parameter's fallback value from the named collection (used when `inherit_from_collection`).
Field readAIParamFromCollection(AIParamKind kind, const NamedCollectionPtr & collection, std::string_view name)
{
    const String key(name);
    switch (kind)
    {
        case AIParamKind::String:
            return Field(collection->get<String>(key));
        case AIParamKind::Float:
            return Field(collection->get<Float64>(key));
        case AIParamKind::UInt:
        {
            UInt64 value = collection->get<UInt64>(key);
            checkUIntFitsInt64(value, name);
            return Field(value);
        }
    }
    std::unreachable();
}

}

FunctionBaseAI::FunctionBaseAI(ContextPtr context_) : context(context_)
{
    if (!getContext()->getSettingsRef()[Setting::allow_experimental_ai_functions])
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
            "AI functions are experimental. Set `allow_experimental_ai_functions` setting to enable it");
}

bool FunctionBaseAI::isStringToStringMap(const IDataType & type)
{
    const auto * map_type = typeid_cast<const DataTypeMap *>(&type);
    return map_type && isString(map_type->getKeyType()) && isString(map_type->getValueType());
}

AIParamSpecs FunctionBaseAI::commonParams()
{
    return {
        /// `credentials` is required, but falls back to the default-credentials setting (handled in resolveAIParams).
        {"credentials", AIParamKind::String, std::nullopt},
        /// `model` is required, but is normally supplied by the named collection.
        {"model", AIParamKind::String, std::nullopt, /*inherit_from_collection=*/ true},
        {"max_tokens", AIParamKind::UInt, Field(DEFAULT_AI_MAX_TOKENS), /*inherit_from_collection=*/ true},
    };
}

AIParamSpecs FunctionBaseAI::allParams() const
{
    auto spec = commonParams();
    auto extra = functionParams();
    spec.insert(spec.end(), extra.begin(), extra.end());
    return spec;
}

namespace
{

const Field & getResolvedAIParam(const AIParamValues & values, std::string_view key)
{
    auto it = values.find(key);
    chassert(it != values.end());
    return it->second;
}

}

String FunctionBaseAI::AIParams::getString(std::string_view key) const
{
    return getResolvedAIParam(values, key).safeGet<String>();
}

Float64 FunctionBaseAI::AIParams::getFloat(std::string_view key) const
{
    return getResolvedAIParam(values, key).safeGet<Float64>();
}

UInt64 FunctionBaseAI::AIParams::getUInt(std::string_view key) const
{
    return getResolvedAIParam(values, key).safeGet<UInt64>();
}

FunctionBaseAI::AIParams FunctionBaseAI::resolveAIParams(
    const ContextPtr & context,
    const ColumnsWithTypeAndName & arguments,
    const AIParamSpecs & spec,
    const String & default_credentials)
{
    /// The parameter map, when present, is the last argument (validated as a const Map(String, String)
    /// by getReturnTypeImpl). Read it into a plain string->string map.
    std::map<String, String, std::less<>> map_values; // STYLE_CHECK_ALLOW_STD_CONTAINERS
    if (!arguments.empty() && isStringToStringMap(*arguments.back().type))
    {
        const auto * map_const = typeid_cast<const ColumnConst *>(arguments.back().column.get());
        if (!map_const)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "AI function parameter map must be a constant");

        const Map & map = (*map_const->getDataColumnPtr())[0].safeGet<Map>();
        for (const auto & element : map)
        {
            const Tuple & kv = element.safeGet<Tuple>();
            const String & key = kv[0].safeGet<String>();
            if (!map_values.emplace(key, kv[1].safeGet<String>()).second)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Duplicate AI function parameter '{}' in the parameter map", key);
        }
    }

    /// Reject unknown keys so typos surface immediately instead of being silently ignored.
    for (const auto & [key, _] : map_values)
    {
        bool known = std::any_of(spec.begin(), spec.end(), [&](const AIParamSpec & p) { return p.name == key; });
        if (!known)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown AI function parameter '{}'", key);
    }

    /// Resolve credentials first: they name the collection everything else is read from.
    String credentials;
    if (auto it = map_values.find("credentials"); it != map_values.end())
        credentials = it->second;
    else
        credentials = default_credentials;

    if (credentials.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "AI function requires credentials: pass 'credentials' in the parameter map or set the default-credentials setting");

    context->checkAccess(AccessType::NAMED_COLLECTION, credentials);
    const auto & collection = NamedCollectionFactory::instance().get(credentials);

    AIParams params;
    params.collection.collection_name = credentials;
    params.collection.provider = collection->getOrDefault<String>("provider", "");
    params.collection.endpoint = collection->getOrDefault<String>("endpoint", "");
    params.collection.api_key = collection->getOrDefault<String>("api_key", "");
    params.collection.api_version = collection->getOrDefault<String>("api_version", "");

    if (params.collection.provider.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "AI named collection '{}' must have 'provider'", credentials);
    if (params.collection.endpoint.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "AI named collection '{}' must have 'endpoint'", credentials);

    context->getRemoteHostFilter().checkURL(Poco::URI(params.collection.endpoint));

    /// Resolve every declared parameter: map override -> named collection (if inherited) -> default.
    for (const auto & p : spec)
    {
        if (p.name == "credentials")
            continue;

        if (auto it = map_values.find(p.name); it != map_values.end())
            params.values.emplace(String(p.name), parseAIParamValue(p.kind, it->second, p.name));
        else if (p.inherit_from_collection && collection->has(String(p.name)))
            params.values.emplace(String(p.name), readAIParamFromCollection(p.kind, collection, p.name));
        else if (p.default_value)
            params.values.emplace(String(p.name), *p.default_value);
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "AI named collection '{}' must have '{}', or it must be passed in the parameter map", credentials, p.name);
    }

    return params;
}

UInt64 FunctionBaseAI::computeRetryBackoffMs(UInt64 initial_delay_ms, UInt64 attempt)
{
    constexpr UInt64 max_retry_delay_ms = 60'000;
    UInt64 delay_ms = std::min(initial_delay_ms, max_retry_delay_ms);
    for (UInt64 i = 0; i < attempt && delay_ms < max_retry_delay_ms; ++i)
        delay_ms = std::min(delay_ms * 2, max_retry_delay_ms);
    return delay_ms;
}

bool FunctionBaseAI::isRetriableProviderError(std::exception_ptr exception)
{
    try
    {
        std::rethrow_exception(exception);
    }
    catch (const AIProviderHTTPException & exception)
    {
        return isRetriableHTTPError(exception.getHTTPStatus());
    }
    catch (const NetException &)
    {
        /// ClickHouse-level network error (e.g. a DNS failure raised by the HTTP connection pool).
        return true;
    }
    catch (const Poco::Net::NetException &)
    {
        /// Connection refused/reset, TLS connect failure, or an unreachable advertised address.
        return true;
    }
    catch (const Poco::TimeoutException &)
    {
        /// Connect or receive timeout.
        return true;
    }
    catch (const Poco::IOException & exception)
    {
        /// Write-side transient I/O failure, e.g. a broken pipe (`EPIPE`) when the peer resets the
        /// connection mid-request. Out-of-file-descriptors (`EMFILE`) is not retriable.
        return exception.code() != POCO_EMFILE;
    }
    catch (...)
    {
        /// Ok: any other exception is a deterministic and non-retrieable error, e.g. a malformed
        /// provider response, bad configuration, JSON parse failure, etc.
        return false;
    }
}

ColumnPtr FunctionBaseAI::executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const
{
    const auto & settings = getContext()->getSettingsRef();
    auto params = resolveAIParams(getContext(), arguments, allParams(), settings[Setting::ai_function_text_default_credentials]);

    String model = params.getString("model");
    UInt64 max_tokens = params.getUInt("max_tokens");
    float temperature = static_cast<float>(params.getFloat("temperature"));

    /// Row-independent validation must run before the zero-row fast path so malformed constant
    /// arguments fail consistently regardless of source size.
    checkSanityBeforeExecuteImpl(arguments, result_type, input_rows_count);
    String system_prompt = sanitizeTextForAI(buildSystemPrompt(arguments, params));
    auto response_format = buildResponseFormat(arguments);
    auto provider = createAIProvider(params.collection.provider, params.collection.endpoint, params.collection.api_key, params.collection.api_version);

    if (input_rows_count == 0)
        return result_type->createColumn();

    /// A Nullable prompt can arrive as `ColumnNullable` or as `ColumnConst(ColumnNullable)` (e.g. `NULL::Nullable(String)`).
    /// `convertToFullColumnIfConst` unwraps the latter into the former, so a single null-map path handles both.
    ColumnPtr prompt_column;
    const ColumnNullable * prompt_nullable = nullptr;
    if (arguments[0].type->isNullable())
    {
        prompt_column = arguments[0].column->convertToFullColumnIfConst();
        prompt_nullable = typeid_cast<const ColumnNullable *>(prompt_column.get());
    }

    UInt64 timeout_sec = settings[Setting::ai_function_request_timeout_sec].value;
    UInt64 max_retries = settings[Setting::ai_function_max_retries].value;
    UInt64 retry_delay_ms = settings[Setting::ai_function_retry_initial_delay_ms].value;

    bool throw_on_error = settings[Setting::ai_function_throw_on_error].value;

    AIQuotaTracker quota(
        settings[Setting::ai_function_max_input_tokens_per_query].value,
        settings[Setting::ai_function_max_output_tokens_per_query].value,
        settings[Setting::ai_function_max_api_calls_per_query].value,
        settings[Setting::ai_function_throw_on_quota_exceeded].value);

    auto timeouts = ConnectionTimeouts::getHTTPTimeouts(settings, getContext()->getServerSettings());
    timeouts.receive_timeout = Poco::Timespan(static_cast<int64_t>(timeout_sec) /*s*/, 0 /*us*/);

    auto result_col = ColumnString::create();
    auto null_map_col = prompt_nullable ? ColumnUInt8::create(input_rows_count, static_cast<UInt8>(0)) : nullptr;

    UInt64 total_api_calls = 0;
    UInt64 total_input_tokens = 0;
    UInt64 total_output_tokens = 0;
    UInt64 rows_processed = 0;
    UInt64 rows_skipped = 0;

    for (size_t i = 0; i < input_rows_count; ++i)
    {
        if (prompt_nullable && prompt_nullable->getNullMapData()[i])
        {
            result_col->insertDefault();
            null_map_col->getData()[i] = 1;
            continue;
        }

        if (quota.checkQuotas())
        {
            result_col->insertDefault();
            ++rows_skipped;
            continue;
        }

        String user_message = sanitizeTextForAI(buildUserMessage(arguments, i));
        String result;
        bool success = false;

        for (UInt64 attempt = 0; attempt <= max_retries; ++attempt)
        {
            /// Check quotas before every request.
            /// Kept outside the `try` so an exception due to `throw_on_quota_exceeded` is not caught by the retry handler.
            if (quota.checkQuotas())
                break;

            try
            {
                AIRequest ai_request;
                ai_request.system_prompt = system_prompt;
                ai_request.user_message = user_message;
                ai_request.response_format = response_format;
                ai_request.model = model;
                ai_request.temperature = temperature;
                ai_request.max_tokens = max_tokens;

                /// update api_calls/quotas before call so failed calls are still added to total
                ++total_api_calls;
                quota.recordAttempt();

                auto ai_response = provider->call(ai_request, timeouts);

                quota.recordTokens(ai_response.input_tokens, ai_response.output_tokens);
                total_input_tokens += ai_response.input_tokens;
                total_output_tokens += ai_response.output_tokens;

                result = postProcessResponse(ai_response.result);
                success = true;
                break;
            }
            catch (...)
            {
                if (attempt < max_retries && isRetriableProviderError(std::current_exception()))
                {
                    std::this_thread::sleep_for(std::chrono::milliseconds(computeRetryBackoffMs(retry_delay_ms, attempt)));
                    continue;
                }

                if (!throw_on_error)
                    break;

                throw;
            }
        }

        result_col->insertData(result.data(), result.size());
        if (success)
            ++rows_processed;
        else
            ++rows_skipped;
    }

    ProfileEvents::increment(ProfileEvents::AIAPICalls, total_api_calls);
    ProfileEvents::increment(ProfileEvents::AIInputTokens, total_input_tokens);
    ProfileEvents::increment(ProfileEvents::AIOutputTokens, total_output_tokens);
    ProfileEvents::increment(ProfileEvents::AIRowsProcessed, rows_processed);
    ProfileEvents::increment(ProfileEvents::AIRowsSkipped, rows_skipped);

    if (result_type->isNullable())
    {
        if (!null_map_col)
            null_map_col = ColumnUInt8::create(input_rows_count, static_cast<UInt8>(0));
        return ColumnNullable::create(std::move(result_col), std::move(null_map_col));
    }
    return result_col;
}

}
