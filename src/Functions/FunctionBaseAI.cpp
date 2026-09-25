#include <Functions/FunctionBaseAI.h>

#include <Access/Common/AccessType.h>
#include <Access/ContextAccess.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Common/Exception.h>
#include <Common/NamedCollections/NamedCollectionsFactory.h>
#include <Common/ProfileEvents.h>
#include <Common/RemoteHostFilter.h>
#include <Common/logger_useful.h>
#include <Common/scope_guard_safe.h>
#include <Core/ServerSettings.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <IO/ConnectionTimeouts.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <base/scope_guard.h>

#include <Poco/Net/IPAddress.h>
#include <Poco/URI.h>

#include <algorithm>
#include <future>
#include <limits>
#include <optional>
#include <utility>

namespace ProfileEvents
{
    extern const Event AIRowsProcessed;
    extern const Event AIRowsSkipped;
}

namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 ai_function_request_timeout_sec;
    extern const SettingsUInt64 ai_function_max_retries;
    extern const SettingsNonZeroUInt64 ai_function_max_concurrent_requests;
    extern const SettingsUInt64 ai_function_retry_initial_delay_ms;
    extern const SettingsBool ai_function_throw_on_error;
    extern const SettingsString ai_function_text_default_credentials;
    extern const SettingsBool ai_function_allow_insecure_endpoint;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

/// Strip control characters (U+0000..U+001F except \t \n \r) that break JSON serialization.
String sanitizeForModel(std::string_view input)
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

/// Whether an endpoint host refers to the local machine.
bool isLoopbackHost(const String & host)
{
    if (host == "localhost")
        return true;
    Poco::Net::IPAddress address;
    if (Poco::Net::IPAddress::tryParse(host, address))
        return address.isLoopback();
    return false;
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
        {
            if (key == "model")
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "This function does not accept 'model' in the parameter map; pass 'model' to the function directly");
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown AI function parameter '{}'", key);
        }
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

    const Poco::URI endpoint_uri(params.collection.endpoint);
    context->getRemoteHostFilter().checkURL(endpoint_uri);

    /// Refuse to send prompts and API keys over an unencrypted connection to a remote host.
    /// Local hosts are exempt, the `ai_function_allow_insecure_endpoint` setting overrides the check.
    if (endpoint_uri.getScheme() != "https"
        && !isLoopbackHost(endpoint_uri.getHost())
        && !context->getSettingsRef()[Setting::ai_function_allow_insecure_endpoint])
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "AI named collection '{}' uses an insecure endpoint '{}': prompts and the API key would be sent to a "
            "remote host over an unencrypted connection. Use an 'https://' endpoint, or set "
            "'ai_function_allow_insecure_endpoint' to allow plaintext requests to remote hosts.",
            credentials, params.collection.endpoint);

    /// A function that does not declare `model` (i.e. `aiEmbed`, which takes it as an argument) must
    /// not silently ignore a `model` defined in the named collection: reject it instead.
    const bool declares_model = std::any_of(spec.begin(), spec.end(), [](const AIParamSpec & p) { return p.name == "model"; });
    if (!declares_model && collection->has("model"))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "AI named collection '{}' defines 'model', which this function does not read from the named collection; "
            "remove it from the collection and pass 'model' to the function directly", credentials);

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

AIRequestPolicy FunctionBaseAI::makeRequestPolicy(const ContextPtr & context)
{
    const auto & settings = context->getSettingsRef();

    AIRequestPolicy policy;
    policy.timeouts = ConnectionTimeouts::getHTTPTimeouts(settings, context->getServerSettings());
    policy.timeouts.receive_timeout
        = Poco::Timespan(static_cast<int64_t>(settings[Setting::ai_function_request_timeout_sec].value) /*s*/, 0 /*us*/);
    policy.max_retries = settings[Setting::ai_function_max_retries].value;
    policy.retry_initial_delay_ms = settings[Setting::ai_function_retry_initial_delay_ms].value;
    policy.throw_on_error = settings[Setting::ai_function_throw_on_error].value;
    return policy;
}

void FunctionBaseAI::insertProcessedResult(IColumn & column, const String & processed) const
{
    column.insertData(processed.data(), processed.size());
}

AIParamSpecs FunctionBaseAI::embeddingParams()
{
    return {
        {"credentials", AIParamKind::String, std::nullopt},
        {"dimensions", AIParamKind::UInt, Field(UInt64(0))},
    };
}

void FunctionBaseAI::embedTexts(
    const std::shared_ptr<IAIProvider> & provider,
    const String & model,
    UInt64 dimensions,
    const String & function_name,
    const VectorWithMemoryTracking<std::string_view> & inputs,
    size_t max_batch_size,
    size_t max_concurrent_requests,
    const AIRequestPolicy & policy,
    const AIQuotaTrackerPtr & quota,
    EmbeddingResult & result)
{
    result.embeddings.resize(inputs.size());

    /// Equivalent to (n + max_batch_size - 1) / max_batch_size but can't overflow
    const size_t num_batches = inputs.empty() ? 0 : 1 + (inputs.size() - 1) / max_batch_size;
    const size_t concurrency = std::min(max_concurrent_requests, num_batches);

    /// Batches go out in waves of size `concurrency` and each completed wave is applied before the next
    /// one starts, so at most that many of this call's requests are in flight at a time.
    VectorWithMemoryTracking<std::future<std::optional<AIEmbeddingResponse>>> wave;
    wave.reserve(concurrency);

    /// Wait for all waves to complete on error, so that API-call and request counts are recorded
    /// They were already dispatched and billed either way.
    SCOPE_EXIT_SAFE({
        for (auto & request : wave)
            if (request.valid())
                request.wait();
    });

    for (size_t wave_begin = 0; wave_begin < num_batches; wave_begin += concurrency)
    {
        const size_t wave_end = std::min(wave_begin + concurrency, num_batches);

        wave.clear();
        /// fire off `concurrency` batches at a time
        for (size_t batch = wave_begin; batch < wave_end; ++batch)
        {
            /// Once the quota is exhausted nothing more is issued, so the batch's slot stays an
            /// empty future and its inputs stay empty.
            if (quota->checkQuotas())
            {
                wave.emplace_back();
                continue;
            }

            const size_t begin = batch * max_batch_size;
            const size_t end = std::min(begin + max_batch_size, inputs.size());

            AIEmbeddingRequest ai_embedding_request;
            ai_embedding_request.model = model;
            ai_embedding_request.dimensions = dimensions;
            ai_embedding_request.function_name = function_name;
            ai_embedding_request.inputs.reserve(end - begin);
            for (size_t k = begin; k < end; ++k)
                ai_embedding_request.inputs.emplace_back(inputs[k]);

            wave.push_back(submitAIRequest(provider, std::move(ai_embedding_request), policy, quota));
        }

        /// get the results for our `concurrency` batches
        for (size_t k = 0; k < wave.size(); ++k)
        {
            const size_t begin = (wave_begin + k) * max_batch_size;
            const size_t end = std::min(begin + max_batch_size, inputs.size());

            /// Nothing when no request was issued for this batch, or when it failed and
            /// `ai_function_throw_on_error` is disabled; either way its inputs stay empty.
            std::optional<AIEmbeddingResponse> ai_embedding_response;
            if (wave[k].valid())
                ai_embedding_response = wave[k].get();

            if (!ai_embedding_response)
            {
                result.texts_skipped += end - begin;
                continue;
            }

            chassert(ai_embedding_response->embeddings.size() == end - begin,
                "Number of inputs does not match number of output embeddings");

            for (size_t j = 0; j < ai_embedding_response->embeddings.size(); ++j)
            {
                result.embeddings[begin + j] = std::move(ai_embedding_response->embeddings[j]);
                ++result.texts_embedded;
            }
        }
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
    String system_prompt = sanitizeForModel(buildSystemPrompt(arguments, params));
    auto response_format = buildResponseFormat(arguments);

    /// Shared with the submitted requests so each one is self-contained and nothing dangles even
    /// if this call stops waiting for a future it handed out.
    std::shared_ptr<IAIProvider> provider = createAIProvider(
        params.collection.provider, params.collection.endpoint, params.collection.api_key, params.collection.api_version);

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

    /// Shared across every AI function call in the query
    auto quota_tracker = getContext()->getAIQuotaTracker();

    const auto policy = makeRequestPolicy(getContext());

    auto result_col = removeNullable(result_type)->createColumn();
    auto null_map_col = prompt_nullable ? ColumnUInt8::create(input_rows_count, static_cast<UInt8>(0)) : nullptr;

    UInt64 rows_processed = 0;
    UInt64 rows_skipped = 0;

    /// Increment ProfileEvents counters upon destruction, to avoid underreporting on error.
    /// The per-request counters (`AIAPICalls`, `AIInputTokens`, `AIOutputTokens`) are incremented by
    /// the submitted requests as they complete.
    SCOPE_EXIT({
        ProfileEvents::increment(ProfileEvents::AIRowsProcessed, rows_processed);
        ProfileEvents::increment(ProfileEvents::AIRowsSkipped, rows_skipped);
    });

    const size_t concurrency = std::min<UInt64>(settings[Setting::ai_function_max_concurrent_requests].value, input_rows_count);

    /// Requests go out in waves of `concurrency` rows, and each completed wave is applied to the
    /// result column in row order. A wave waits for its slowest request before the next one starts,
    /// which gives up a little throughput next to a sliding window, but keeps row ordering, quota
    /// accounting and error propagation identical to issuing the requests one at a time.
    VectorWithMemoryTracking<std::future<std::optional<AIResponse>>> wave;
    wave.reserve(concurrency);

    /// Wait for all waves to complete on error, so that API-call and request counts are recorded
    /// They were already dispatched and billed either way.
    SCOPE_EXIT_SAFE({
        for (auto & request : wave)
            if (request.valid())
                request.wait();
    });

    for (size_t wave_begin = 0; wave_begin < input_rows_count; wave_begin += concurrency)
    {
        const size_t wave_end = std::min(wave_begin + concurrency, input_rows_count);

        wave.clear();
        for (size_t row = wave_begin; row < wave_end; ++row)
        {
            /// A NULL prompt produces NULL, and once the quota is exhausted a row keeps its default
            /// value. Neither issues a request, so the row's slot stays an empty future.
            if ((prompt_nullable && prompt_nullable->getNullMapData()[row]) || quota_tracker->checkQuotas())
            {
                wave.emplace_back();
                continue;
            }

            AIRequest ai_request;
            ai_request.system_prompt = system_prompt;
            ai_request.user_message = sanitizeForModel(buildUserMessage(arguments, row));
            ai_request.response_format = response_format;
            ai_request.model = model;
            ai_request.temperature = temperature;
            ai_request.max_tokens = max_tokens;
            ai_request.function_name = getName();

            wave.push_back(submitAIRequest(provider, std::move(ai_request), policy, quota_tracker));
        }

        for (size_t k = 0; k < wave.size(); ++k)
        {
            const size_t row = wave_begin + k;

            /// A NULL prompt produces NULL without a request.
            if (prompt_nullable && prompt_nullable->getNullMapData()[row])
            {
                result_col->insertDefault();
                null_map_col->getData()[row] = 1;
                continue;
            }

            /// Nothing when no request was issued because the API-call quota was exhausted, or when
            /// the request failed and `ai_function_throw_on_error` is disabled; either way the row
            /// keeps its default value.
            std::optional<AIResponse> ai_response;
            if (wave[k].valid())
                ai_response = wave[k].get();

            if (!ai_response)
            {
                result_col->insertDefault();
                ++rows_skipped;
                continue;
            }

            insertProcessedResult(*result_col, postProcessResponse(ai_response->result));
            ++rows_processed;
        }
    }

    if (result_type->isNullable())
    {
        if (!null_map_col)
            null_map_col = ColumnUInt8::create(input_rows_count, static_cast<UInt8>(0));
        return ColumnNullable::create(std::move(result_col), std::move(null_map_col));
    }
    return result_col;
}

}
