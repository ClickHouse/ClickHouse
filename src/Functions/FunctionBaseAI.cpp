#include <Functions/FunctionBaseAI.h>
#include <Common/ProfileEvents.h>
#include <Common/Throttler.h>
#include <Common/ThreadPool.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Common/NamedCollections/NamedCollectionsFactory.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypeString.h>
#include <IO/ConnectionTimeouts.h>
#include <Core/Settings.h>
#include <Core/ServerSettings.h>
#include <Common/CurrentMetrics.h>

#include <unordered_map>

namespace CurrentMetrics
{
    extern const Metric AIThreads;
    extern const Metric AIThreadsActive;
    extern const Metric AIThreadsScheduled;
}

namespace ProfileEvents
{
    extern const Event AIInputTokens;
    extern const Event AIOutputTokens;
    extern const Event AICacheHits;
    extern const Event AICacheMisses;
    extern const Event AIAPICalls;
    extern const Event AIRowsProcessed;
    extern const Event AIRowsSkipped;
}

namespace DB
{

namespace Setting
{
    extern const SettingsBool allow_experimental_ai_functions;
    extern const SettingsString default_ai_provider;
    extern const SettingsUInt64 ai_request_timeout_sec;
    extern const SettingsUInt64 ai_max_concurrent_requests;
    extern const SettingsUInt64 ai_max_rps;
    extern const SettingsUInt64 ai_max_retries;
    extern const SettingsUInt64 ai_retry_initial_delay_ms;
    extern const SettingsUInt64 ai_cache_ttl_sec;
    extern const SettingsString ai_on_error;
    extern const SettingsUInt64 ai_max_rows_per_query;
    extern const SettingsUInt64 ai_max_input_tokens_per_query;
    extern const SettingsUInt64 ai_max_output_tokens_per_query;
    extern const SettingsUInt64 ai_max_api_calls_per_query;
    extern const SettingsString ai_on_quota_exceeded;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int RECEIVED_ERROR_FROM_REMOTE_IO_SERVER;
    extern const int SUPPORT_IS_DISABLED;
}

FunctionBaseAI::FunctionBaseAI(ContextPtr context_) : context_weak(context_)
{
    if (!getContext()->getSettingsRef()[Setting::allow_experimental_ai_functions])
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
            "AI functions are experimental. Set `allow_experimental_ai_functions` setting to enable it");
}

bool FunctionBaseAI::hasNamedCollectionArg(const ColumnsWithTypeAndName & arguments) const
{
    if (arguments.empty())
        return false;

    if (!isString(arguments[0].type))
        return false;

    const auto * col_const = typeid_cast<const ColumnConst *>(arguments[0].column.get());
    if (!col_const)
        return false;

    String first_arg = col_const->getValue<String>();
    if (first_arg.empty())
        return false;

    return NamedCollectionFactory::instance().exists(first_arg);
}

size_t FunctionBaseAI::getFirstDataArgIndex(const ColumnsWithTypeAndName & arguments) const
{
    return hasNamedCollectionArg(arguments) ? 1 : 0;
}

FunctionBaseAI::ResolvedConfig FunctionBaseAI::resolveConfig(const ColumnsWithTypeAndName & arguments) const
{
    ResolvedConfig config;
    const auto & settings = getContext()->getSettingsRef();

    String collection_name;
    if (hasNamedCollectionArg(arguments))
    {
        const auto * col_const = typeid_cast<const ColumnConst *>(arguments[0].column.get());
        collection_name = col_const->getValue<String>();
    }
    else
    {
        collection_name = settings[Setting::default_ai_provider].value;
    }

    if (collection_name.empty())
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "No AI named collection specified and default_ai_provider is not set");
    }

    const auto & nc = NamedCollectionFactory::instance().get(collection_name);

    config.provider = nc->getOrDefault<String>("provider", DEFAULT_AI_PROVIDER);
    config.endpoint = nc->getOrDefault<String>("endpoint", "");
    config.model = nc->getOrDefault<String>("model", "");
    config.api_key = nc->getOrDefault<String>("api_key", "");
    config.max_tokens = nc->getOrDefault<UInt64>("max_tokens", DEFAULT_AI_MAX_TOKENS);
    config.temperature = defaultTemperature();

    if (config.endpoint.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "AI named collection '{}' must have 'endpoint'", collection_name);
    if (config.model.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "AI named collection '{}' must have 'model'", collection_name);
    if (config.api_key.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "AI named collection '{}' must have 'api_key'", collection_name);

    return config;
}

float FunctionBaseAI::resolveTemperature(const ColumnsWithTypeAndName & arguments, const ResolvedConfig & config) const
{
    if (arguments.empty())
        return config.temperature;

    const auto & last_arg = arguments.back();
    if (isFloat(last_arg.type))
    {
        const auto * col_const = typeid_cast<const ColumnConst *>(last_arg.column.get());
        if (col_const)
            return static_cast<float>((*col_const)[0].safeGet<Float64>());
    }

    return config.temperature;
}

ColumnPtr FunctionBaseAI::executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const
{
    auto config = resolveConfig(arguments);
    auto provider = createAIProvider(config.provider, config.endpoint, config.api_key);
    float temperature = resolveTemperature(arguments, config);

    const auto & settings = getContext()->getSettingsRef();
    UInt64 timeout_sec = settings[Setting::ai_request_timeout_sec].value;
    UInt64 max_concurrent = settings[Setting::ai_max_concurrent_requests].value;
    UInt64 max_rps = settings[Setting::ai_max_rps].value;
    UInt64 max_retries = settings[Setting::ai_max_retries].value;
    UInt64 retry_delay_ms = settings[Setting::ai_retry_initial_delay_ms].value;
    UInt64 cache_ttl = settings[Setting::ai_cache_ttl_sec].value;

    auto quota = std::make_shared<AIQuotaTracker>(
        settings[Setting::ai_max_rows_per_query].value,
        settings[Setting::ai_max_input_tokens_per_query].value,
        settings[Setting::ai_max_output_tokens_per_query].value,
        settings[Setting::ai_max_api_calls_per_query].value,
        String(settings[Setting::ai_on_quota_exceeded].value),
        String(settings[Setting::ai_on_error].value));

    auto timeouts = ConnectionTimeouts::getHTTPTimeouts(settings, getContext()->getServerSettings());
    timeouts.receive_timeout = Poco::Timespan(static_cast<int64_t>(timeout_sec) /*s*/, 0 /*us*/);

    auto throttler = std::make_shared<Throttler>("ai_rps", max_rps);

    String system_prompt = buildSystemPrompt(arguments);
    String response_format = buildResponseFormatJSON(arguments);

    auto result_col = ColumnString::create();

    std::unordered_map<UInt128, std::vector<size_t>, UInt128Hash> dedup_map;

    for (size_t i = 0; i < input_rows_count; ++i)
    {
        const auto & text_col = arguments[getFirstDataArgIndex(arguments)].column;
        if (text_col->isNullAt(i))
            continue;

        String user_message = buildUserMessage(arguments, i);
        std::vector<String> cache_args = {user_message, system_prompt};
        UInt128 key = AIResultCache::buildKey(functionName(), config.model, temperature, cache_args);
        dedup_map[key].push_back(i);
    }

    std::unordered_map<UInt128, String, UInt128Hash> results;
    std::mutex results_mutex;

    std::atomic<UInt64> total_api_calls{0};
    std::atomic<UInt64> total_input_tokens{0};
    std::atomic<UInt64> total_output_tokens{0};
    UInt64 cache_hits = 0;
    UInt64 cache_misses = 0;

    auto & ai_cache = AIResultCache::instance();
    std::vector<std::pair<UInt128, size_t>> to_dispatch;

    for (auto & [key, rows] : dedup_map)
    {
        auto cached_entry = ai_cache.get(key);
        if (cached_entry && std::chrono::system_clock::now() <= cached_entry->expires_at)
        {
            cached_entry->hit_count.fetch_add(1, std::memory_order_relaxed);
            std::lock_guard lock(results_mutex);
            results[key] = cached_entry->result;
            ++cache_hits;
            quota->rows_processed.fetch_add(rows.size(), std::memory_order_relaxed);
        }
        else
        {
            to_dispatch.emplace_back(key, rows[0]);
            ++cache_misses;
        }
    }

    if (!to_dispatch.empty())
    {
        auto pool = std::make_unique<ThreadPool>(CurrentMetrics::AIThreads, CurrentMetrics::AIThreadsActive, CurrentMetrics::AIThreadsScheduled, max_concurrent, max_concurrent, max_concurrent);

        for (auto & [dispatch_key, representative_row] : to_dispatch)
        {
            UInt64 rows_for_key = dedup_map[dispatch_key].size();
            if (!quota->checkBeforeDispatch(0, rows_for_key))
            {
                std::lock_guard lock(results_mutex);
                results[dispatch_key] = "";
                continue;
            }

            pool->scheduleOrThrowOnError([&, dk = dispatch_key, row = representative_row]
            {
                String user_message = buildUserMessage(arguments, row);

                for (UInt64 attempt = 0; attempt <= max_retries; ++attempt)
                {
                    try
                    {
                        if (max_rps > 0)
                            throttler->throttle(1, 0);

                        AIRequest ai_request;
                        ai_request.system_prompt = system_prompt;
                        ai_request.user_message = user_message;
                        ai_request.response_format_json = response_format;
                        ai_request.model = config.model;
                        ai_request.temperature = temperature;
                        ai_request.max_tokens = config.max_tokens;

                        auto ai_response = provider->call(ai_request, timeouts);

                        quota->recordResponse(ai_response.input_tokens, ai_response.output_tokens);
                        total_input_tokens.fetch_add(ai_response.input_tokens, std::memory_order_relaxed);
                        total_output_tokens.fetch_add(ai_response.output_tokens, std::memory_order_relaxed);
                        total_api_calls.fetch_add(1, std::memory_order_relaxed);

                        String processed = postProcessResponse(ai_response.result);

                        if (cache_ttl > 0 && ai_response.finish_reason != "length")
                        {
                            auto entry = std::make_shared<AICacheEntry>();
                            entry->result = processed;
                            entry->function_name = functionName();
                            entry->model = config.model;
                            entry->result_size_bytes = processed.size();
                            entry->created_at = std::chrono::system_clock::now();
                            entry->expires_at = entry->created_at + std::chrono::seconds(cache_ttl);
                            ai_cache.set(dk, entry);
                        }

                        {
                            std::lock_guard lock(results_mutex);
                            results[dk] = processed;
                        }
                        return;
                    }
                    catch (const Exception & e)
                    {
                        if (attempt < max_retries && e.code() == ErrorCodes::RECEIVED_ERROR_FROM_REMOTE_IO_SERVER)
                        {
                            std::this_thread::sleep_for(std::chrono::milliseconds(retry_delay_ms * (1ULL << attempt)));
                            continue;
                        }

                        if (quota->handleRowError())
                        {
                            std::lock_guard lock(results_mutex);
                            results[dk] = "";
                            return;
                        }
                        throw;
                    }
                }
            });
        }

        pool->wait();
    }

    ProfileEvents::increment(ProfileEvents::AICacheHits, cache_hits);
    ProfileEvents::increment(ProfileEvents::AICacheMisses, cache_misses);
    ProfileEvents::increment(ProfileEvents::AIAPICalls, total_api_calls.load());
    ProfileEvents::increment(ProfileEvents::AIInputTokens, total_input_tokens.load());
    ProfileEvents::increment(ProfileEvents::AIOutputTokens, total_output_tokens.load());

    std::vector<String> ordered_results(input_rows_count);
    UInt64 rows_processed_count = 0;
    UInt64 rows_skipped_count = 0;

    for (const auto & [key, rows] : dedup_map)
    {
        auto it = results.find(key);
        String value;
        if (it != results.end())
            value = it->second;

        for (size_t row : rows)
        {
            ordered_results[row] = value;
            if (value.empty() && quota->isQuotaExceeded())
                ++rows_skipped_count;
            else
                ++rows_processed_count;
        }
    }

    ProfileEvents::increment(ProfileEvents::AIRowsProcessed, rows_processed_count);
    ProfileEvents::increment(ProfileEvents::AIRowsSkipped, rows_skipped_count);

    for (size_t i = 0; i < input_rows_count; ++i)
        result_col->insertData(ordered_results[i].data(), ordered_results[i].size());

    return result_col;
}

}
