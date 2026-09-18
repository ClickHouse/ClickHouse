#include <Functions/FunctionBaseAI.h>
#include <Access/Common/AccessType.h>
#include <Access/ContextAccess.h>
#include <Common/ProfileEvents.h>
#include <Common/CurrentMetrics.h>
#include <Common/setThreadName.h>
#include <Common/threadPoolCallbackRunner.h>
#include <base/scope_guard.h>
#include <Common/Exception.h>
#include <Common/NetException.h>
#include <Poco/Net/NetException.h>
#include <algorithm>
#include <atomic>
#include <exception>
#include <thread>
#include <utility>
#include <Common/logger_useful.h>
#include <Common/NamedCollections/NamedCollectionsFactory.h>
#include <Common/RemoteHostFilter.h>
#include <Poco/URI.h>
#include <Poco/Net/IPAddress.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeMap.h>
#include <IO/ConnectionTimeouts.h>
#include <IO/HTTPCommon.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromString.h>
#include <Core/Settings.h>
#include <Core/ServerSettings.h>
#include <limits>

namespace CurrentMetrics
{
    extern const Metric AIFunctionThreads;
    extern const Metric AIFunctionThreadsActive;
    extern const Metric AIFunctionThreadsScheduled;
}

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
    extern const int AI_PROVIDER_RESPONSE_TRUNCATED;
    extern const int AI_PROVIDER_RESPONSE_INCOMPLETE;
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
    IAIProvider & provider,
    const String & model,
    UInt64 dimensions,
    const String & function_name,
    const VectorWithMemoryTracking<std::string_view> & inputs,
    size_t max_batch_size,
    size_t max_concurrent_requests,
    UInt64 max_retries,
    UInt64 retry_delay_ms,
    bool throw_on_error,
    AIQuotaTracker & quota,
    const ConnectionTimeouts & timeouts,
    EmbeddingResult & result)
{
    result.embeddings.resize(inputs.size());

    UInt64 api_calls = 0;
    UInt64 input_tokens = 0;

    /// Increment ProfileEvents counters upon destruction, to avoid underreporting on error
    SCOPE_EXIT({
        ProfileEvents::increment(ProfileEvents::AIAPICalls, api_calls);
        ProfileEvents::increment(ProfileEvents::AIInputTokens, input_tokens);
    });

    /// Batch boundaries are fixed up front so the batches can be issued concurrently while each one
    /// still writes only into its own slice of `result.embeddings`.
    struct Batch
    {
        size_t begin;
        size_t end;
    };
    std::vector<Batch> batches; // STYLE_CHECK_ALLOW_STD_CONTAINERS
    for (size_t batch_start = 0; batch_start < inputs.size(); batch_start += max_batch_size)
        batches.push_back({batch_start, std::min(batch_start + max_batch_size, inputs.size())});

    std::atomic<UInt64> atomic_api_calls = 0;
    std::atomic<UInt64> atomic_input_tokens = 0;
    std::atomic<UInt64> atomic_texts_embedded = 0;
    std::atomic<UInt64> atomic_texts_skipped = 0;

    /// Accumulate into the caller's `result` even when a batch throws, so what completed stays visible.
    SCOPE_EXIT({
        result.texts_embedded += atomic_texts_embedded.load();
        result.texts_skipped += atomic_texts_skipped.load();
        api_calls += atomic_api_calls.load();
        input_tokens += atomic_input_tokens.load();
    });

    /// Issue one batch, with retries. Safe to run concurrently for the same reasons as the per-row
    /// requests of the text functions, plus each batch owning a disjoint output range.
    auto process_batch = [&](const Batch & batch)
    {
        if (quota.checkQuotas())
        {
            atomic_texts_skipped.fetch_add(batch.end - batch.begin, std::memory_order_relaxed);
            return;
        }

        AIEmbeddingRequest ai_embedding_request;
        ai_embedding_request.model = model;
        ai_embedding_request.dimensions = dimensions;
        ai_embedding_request.function_name = function_name;
        ai_embedding_request.inputs.reserve(batch.end - batch.begin);
        for (size_t k = batch.begin; k < batch.end; ++k)
            ai_embedding_request.inputs.emplace_back(inputs[k]);

        AIEmbeddingResponse ai_embedding_response;
        bool batch_ok = false;
        for (UInt64 attempt = 0; attempt <= max_retries; ++attempt)
        {
            /// Reserve an API-call slot before each request; this also performs a quota check.
            /// Kept outside the `try` so a `throw_on_quota_exceeded` exception isn't caught by the retry handler.
            if (!quota.recordApiCall())
                break;

            try
            {
                /// Count the call before issuing it, so a failed request is still counted.
                atomic_api_calls.fetch_add(1, std::memory_order_relaxed);
                SCOPE_EXIT({
                    atomic_input_tokens.fetch_add(ai_embedding_response.input_tokens, std::memory_order_relaxed);
                    quota.recordTokens(ai_embedding_response.input_tokens, 0);
                });
                provider.embed(ai_embedding_request, timeouts, ai_embedding_response);
                batch_ok = true;
                break;
            }
            catch (...)
            {
                if (attempt < max_retries && isRetriableProviderError(std::current_exception()))
                {
                    std::this_thread::sleep_for(std::chrono::milliseconds(computeRetryBackoffMs(retry_delay_ms, attempt)));
                    continue;
                }

                if (!throw_on_error) /// Skip this batch, its inputs stay empty.
                    break;

                throw;
            }
        }

        if (!batch_ok)
        {
            atomic_texts_skipped.fetch_add(batch.end - batch.begin, std::memory_order_relaxed);
            return;
        }

        chassert(ai_embedding_response.embeddings.size() == ai_embedding_request.inputs.size(),
            "Number of inputs does not match number of output embeddings");

        for (size_t k = 0; k < ai_embedding_response.embeddings.size(); ++k)
        {
            result.embeddings[batch.begin + k] = std::move(ai_embedding_response.embeddings[k]);
            atomic_texts_embedded.fetch_add(1, std::memory_order_relaxed);
        }
    };

    const size_t concurrency = std::min(max_concurrent_requests, batches.size());
    if (concurrency > 1)
    {
        ThreadPool pool(
            CurrentMetrics::AIFunctionThreads,
            CurrentMetrics::AIFunctionThreadsActive,
            CurrentMetrics::AIFunctionThreadsScheduled,
            concurrency);
        ThreadPoolCallbackRunnerLocal<void> runner(pool, ThreadName::AI_FUNCTION_REQUEST);
        runner.reserve(batches.size());
        for (const auto & batch : batches)
            runner.enqueueAndKeepTrack([&process_batch, batch] { process_batch(batch); });
        runner.waitForAllToFinishAndRethrowFirstError();
    }
    else
    {
        for (const auto & batch : batches)
            process_batch(batch);
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

    /// Shared across every AI function call in the query
    auto quota_tracker = getContext()->getAIQuotaTracker();

    auto timeouts = ConnectionTimeouts::getHTTPTimeouts(settings, getContext()->getServerSettings());
    timeouts.receive_timeout = Poco::Timespan(static_cast<int64_t>(timeout_sec) /*s*/, 0 /*us*/);

    auto result_col = removeNullable(result_type)->createColumn();
    auto null_map_col = prompt_nullable ? ColumnUInt8::create(input_rows_count, static_cast<UInt8>(0)) : nullptr;

    /// Requests may run concurrently (see `ai_function_max_concurrent_requests`), so the counters they
    /// touch are atomic. `rows_processed` / `rows_skipped` are only ever updated while applying a
    /// completed wave to the result column, which is single-threaded, so they stay plain.
    std::atomic<UInt64> total_api_calls = 0;
    std::atomic<UInt64> total_input_tokens = 0;
    std::atomic<UInt64> total_output_tokens = 0;
    UInt64 rows_processed = 0;
    UInt64 rows_skipped = 0;

    /// Increment ProfileEvents counters upon destruction, to avoid underreporting on error
    SCOPE_EXIT({
        ProfileEvents::increment(ProfileEvents::AIAPICalls, total_api_calls.load());
        ProfileEvents::increment(ProfileEvents::AIInputTokens, total_input_tokens.load());
        ProfileEvents::increment(ProfileEvents::AIOutputTokens, total_output_tokens.load());
        ProfileEvents::increment(ProfileEvents::AIRowsProcessed, rows_processed);
        ProfileEvents::increment(ProfileEvents::AIRowsSkipped, rows_skipped);
    });

    /// One row's outcome, produced possibly on a request thread and applied to the result column
    /// afterwards, in row order.
    struct RowOutcome
    {
        bool prompt_is_null = false;
        bool success = false;
        String result;
    };

    /// Issue the request for one row, with retries, and record its outcome. Safe to run concurrently:
    /// the quota tracker is internally synchronized, a provider holds only const configuration and
    /// opens its own HTTP session per call, and the counters touched here are atomic.
    auto process_row = [&](size_t row, RowOutcome & outcome)
    {
        outcome = RowOutcome{};

        if (prompt_nullable && prompt_nullable->getNullMapData()[row])
        {
            outcome.prompt_is_null = true;
            return;
        }

        if (quota_tracker->checkQuotas())
            return;

        String user_message = sanitizeForModel(buildUserMessage(arguments, row));

        for (UInt64 attempt = 0; attempt <= max_retries; ++attempt)
        {
            /// Reserve an API-call slot before each request; this also performs a quota check.
            /// Kept outside the `try` so a `throw_on_quota_exceeded` exception isn't caught by the retry handler.
            if (!quota_tracker->recordApiCall())
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
                ai_request.function_name = getName();

                total_api_calls.fetch_add(1, std::memory_order_relaxed);

                AIResponse ai_response;
                SCOPE_EXIT({
                    quota_tracker->recordTokens(ai_response.input_tokens, ai_response.output_tokens);
                    total_input_tokens.fetch_add(ai_response.input_tokens, std::memory_order_relaxed);
                    total_output_tokens.fetch_add(ai_response.output_tokens, std::memory_order_relaxed);
                });
                provider->call(ai_request, timeouts, ai_response);

                /// `raw_finish_reason` is provider-controlled text; sanitize control characters before
                /// interpolating it into an exception message that reaches the logs and `system.query_log`.
                const String safe_finish_reason = sanitizeForLog(ai_response.raw_finish_reason);

                /// Reject incomplete responses, throw plain DB::Exception so it is classified as non-retriable
                switch (ai_response.finish_reason)
                {
                    case FinishReason::Complete:
                    case FinishReason::Unknown: /// Don't throw on Unknown, could be new valid reason in new API version
                        break;
                    case FinishReason::Truncated:
                        /// Differentiate between model hitting our output cap and exhausting its context window
                        throw Exception(
                            ErrorCodes::AI_PROVIDER_RESPONSE_TRUNCATED,
                            "AI provider returned a truncated response (finish_reason='{}'): {}",
                            safe_finish_reason,
                            ai_response.raw_finish_reason == "model_context_window_exceeded"
                                ? "the model ran out of context window before completing its answer. "
                                  "Reduce the input or use a model with a larger context window."
                                : "the model hit the output token limit before completing its answer. "
                                  "Increase max_tokens or reduce the input.");
                    case FinishReason::ContentFilter:
                        throw Exception(
                            ErrorCodes::AI_PROVIDER_RESPONSE_INCOMPLETE,
                            "AI provider withheld or filtered the response (finish_reason='{}'): the returned answer "
                            "is incomplete.",
                            safe_finish_reason);
                    case FinishReason::RequiresAction:
                        throw Exception(
                            ErrorCodes::AI_PROVIDER_RESPONSE_INCOMPLETE,
                            "AI provider stopped expecting further caller action (finish_reason='{}') instead of "
                            "returning a completed answer.",
                            safe_finish_reason);
                }

                outcome.result = postProcessResponse(ai_response.result);
                outcome.success = true;
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
    };

    /// Requests go out in waves of `concurrency` rows, and each completed wave is applied to the
    /// result column in row order. A wave waits for its slowest request before the next one starts,
    /// which gives up a little throughput next to a sliding window, but keeps row ordering, quota
    /// accounting and error propagation identical to the sequential path.
    const size_t concurrency
        = std::min<UInt64>(settings[Setting::ai_function_max_concurrent_requests].value, input_rows_count);

    std::vector<RowOutcome> wave(concurrency); // STYLE_CHECK_ALLOW_STD_CONTAINERS

    std::optional<ThreadPool> request_pool;
    std::optional<ThreadPoolCallbackRunnerLocal<void>> request_runner;
    if (concurrency > 1)
    {
        request_pool.emplace(
            CurrentMetrics::AIFunctionThreads,
            CurrentMetrics::AIFunctionThreadsActive,
            CurrentMetrics::AIFunctionThreadsScheduled,
            concurrency);
        request_runner.emplace(*request_pool, ThreadName::AI_FUNCTION_REQUEST);
        request_runner->reserve(concurrency);
    }

    for (size_t wave_begin = 0; wave_begin < input_rows_count; wave_begin += concurrency)
    {
        const size_t wave_size = std::min(concurrency, input_rows_count - wave_begin);

        if (request_runner)
        {
            for (size_t k = 0; k < wave_size; ++k)
                request_runner->enqueueAndKeepTrack([&process_row, &wave, k, row = wave_begin + k] { process_row(row, wave[k]); });
            request_runner->waitForAllToFinishAndRethrowFirstError();
        }
        else
        {
            for (size_t k = 0; k < wave_size; ++k)
                process_row(wave_begin + k, wave[k]);
        }

        for (size_t k = 0; k < wave_size; ++k)
        {
            const RowOutcome & outcome = wave[k];
            if (outcome.prompt_is_null)
            {
                result_col->insertDefault();
                null_map_col->getData()[wave_begin + k] = 1;
            }
            else if (outcome.success)
            {
                insertProcessedResult(*result_col, outcome.result);
                ++rows_processed;
            }
            else
            {
                result_col->insertDefault();
                ++rows_skipped;
            }
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
