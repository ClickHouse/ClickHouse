#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/LLM/ILLMProvider.h>
#include <Functions/LLM/LLMResultCache.h>
#include <Functions/LLM/LLMQuotaTracker.h>

#include <Common/ProfileEvents.h>
#include <Common/Throttler.h>
#include <Common/ThreadPool.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Common/NamedCollections/NamedCollectionsFactory.h>
#include <Common/CurrentMetrics.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>

#include <IO/ConnectionTimeouts.h>
#include <Core/Settings.h>
#include <Core/ServerSettings.h>
#include <Interpreters/Context.h>

#include <unordered_map>
#include <unordered_set>
#include <sstream>

namespace ProfileEvents
{
    extern const Event LLMInputTokens;
    extern const Event LLMCacheHits;
    extern const Event LLMCacheMisses;
    extern const Event LLMAPICalls;
    extern const Event LLMRowsProcessed;
    extern const Event LLMRowsSkipped;
}

namespace DB
{

namespace Setting
{
    extern const SettingsString default_llm_resource;
    extern const SettingsUInt64 llm_request_timeout_sec;
    extern const SettingsUInt64 llm_max_concurrent_requests;
    extern const SettingsUInt64 llm_max_rps;
    extern const SettingsUInt64 llm_max_retries;
    extern const SettingsUInt64 llm_retry_initial_delay_ms;
    extern const SettingsUInt64 llm_cache_ttl_sec;
    extern const SettingsString llm_on_error;
    extern const SettingsUInt64 llm_max_rows_per_query;
    extern const SettingsUInt64 llm_max_input_tokens_per_query;
    extern const SettingsUInt64 llm_max_output_tokens_per_query;
    extern const SettingsUInt64 llm_max_api_calls_per_query;
    extern const SettingsString llm_on_quota_exceeded;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int RECEIVED_ERROR_FROM_REMOTE_IO_SERVER;
}

namespace
{

/// Serialize a float vector as a compact string for caching.
String serializeEmbedding(const std::vector<Float32> & vec)
{
    String result;
    result.resize(vec.size() * sizeof(Float32));
    memcpy(result.data(), vec.data(), result.size());
    return result;
}

std::vector<Float32> deserializeEmbedding(const String & data)
{
    size_t count = data.size() / sizeof(Float32);
    std::vector<Float32> vec(count);
    memcpy(vec.data(), data.data(), data.size());
    return vec;
}

class FunctionLLMGenerateEmbedding final : public IFunction
{
public:
    static constexpr auto name = "LLMGenerateEmbedding";
    static FunctionPtr create(ContextPtr ctx) { return std::make_shared<FunctionLLMGenerateEmbedding>(std::move(ctx)); }
    explicit FunctionLLMGenerateEmbedding(ContextPtr context_) : context(std::move(context_)) {}

    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    bool isStateful() const override { return true; }
    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return false; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return false; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() < 2 || arguments.size() > 3)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} requires 2-3 arguments: [collection,] text, dimensions", name);
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat32>());
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
    {
        const auto & settings = context->getSettingsRef();

        String collection_name = settings[Setting::default_llm_resource].value;
        if (collection_name.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "No LLM named collection specified and default_llm_resource is not set");

        const auto & nc = NamedCollectionFactory::instance().get(collection_name);
        String provider_name = nc->getOrDefault<String>("provider", "openai");
        String endpoint_val = nc->getOrDefault<String>("endpoint", "");
        String model = nc->getOrDefault<String>("model", "");
        String api_key = nc->getOrDefault<String>("api_key", "");

        if (endpoint_val.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "LLM named collection '{}' must have 'endpoint'", collection_name);
        if (model.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "LLM named collection '{}' must have 'model'", collection_name);
        if (api_key.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "LLM named collection '{}' must have 'api_key'", collection_name);

        auto provider = createLLMProvider(provider_name, endpoint_val, api_key);

        size_t text_arg_idx = 0;
        size_t dim_arg_idx = 1;
        if (arguments.size() == 3)
        {
            text_arg_idx = 1;
            dim_arg_idx = 2;
        }

        const auto * dim_const = checkAndGetColumn<ColumnConst>(arguments[dim_arg_idx].column.get());
        if (!dim_const)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Dimensions argument must be a constant");
        UInt64 dimensions = dim_const->getUInt(0);

        UInt64 timeout_sec = settings[Setting::llm_request_timeout_sec].value;
        UInt64 max_concurrent = settings[Setting::llm_max_concurrent_requests].value;
        UInt64 max_rps = settings[Setting::llm_max_rps].value;
        UInt64 max_retries = settings[Setting::llm_max_retries].value;
        UInt64 retry_delay_ms = settings[Setting::llm_retry_initial_delay_ms].value;
        UInt64 cache_ttl = settings[Setting::llm_cache_ttl_sec].value;

        auto quota = std::make_shared<LLMQuotaTracker>(
            settings[Setting::llm_max_rows_per_query].value,
            settings[Setting::llm_max_input_tokens_per_query].value,
            settings[Setting::llm_max_output_tokens_per_query].value,
            settings[Setting::llm_max_api_calls_per_query].value,
            String(settings[Setting::llm_on_quota_exceeded].value),
            String(settings[Setting::llm_on_error].value));

        auto timeouts = ConnectionTimeouts::getHTTPTimeouts(settings, context->getServerSettings());
        timeouts.receive_timeout = Poco::Timespan(static_cast<long>(timeout_sec), 0);
        auto throttler = std::make_shared<Throttler>(max_rps);

        std::unordered_map<UInt128, std::vector<size_t>, UInt128Hash> dedup_map;
        std::unordered_set<size_t> null_input_rows;

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const auto & text_col = arguments[text_arg_idx].column;
            if (text_col->isNullAt(i))
            {
                null_input_rows.insert(i);
                continue;
            }

            String text(text_col->getDataAt(i));
            std::vector<String> cache_args = {text, std::to_string(dimensions)};
            UInt128 key = LLMResultCache::buildKey(name, model, 0, cache_args);
            dedup_map[key].push_back(i);
        }

        std::unordered_map<UInt128, std::vector<Float32>, UInt128Hash> results;
        std::mutex results_mutex;

        std::atomic<UInt64> total_api_calls{0};
        std::atomic<UInt64> total_input_tokens{0};
        UInt64 cache_hits = 0;
        UInt64 cache_misses = 0;

        auto & cache = LLMResultCache::instance();
        std::vector<std::pair<UInt128, size_t>> to_dispatch;

        for (auto & [key, rows] : dedup_map)
        {
            auto cached = cache.get(key);
            if (cached && std::chrono::system_clock::now() <= cached->expires_at)
            {
                cached->hit_count.fetch_add(1, std::memory_order_relaxed);
                std::lock_guard lock(results_mutex);
                results[key] = deserializeEmbedding(cached->result);
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
            auto pool = std::make_unique<ThreadPool>(
                CurrentMetrics::end(), CurrentMetrics::end(), CurrentMetrics::end(),
                max_concurrent, max_concurrent, max_concurrent);

            for (auto & [dispatch_key, representative_row] : to_dispatch)
            {
                UInt64 rows_for_key = dedup_map[dispatch_key].size();
                if (!quota->checkBeforeDispatch(0, rows_for_key))
                {
                    std::lock_guard lock(results_mutex);
                    results[dispatch_key] = {};
                    continue;
                }

                pool->scheduleOrThrowOnError([&, dk = dispatch_key, row = representative_row]
                {
                    String text(arguments[text_arg_idx].column->getDataAt(row));

                    for (UInt64 attempt = 0; attempt <= max_retries; ++attempt)
                    {
                        try
                        {
                            if (max_rps > 0)
                                throttler->throttle(1, 0);

                            LLMEmbeddingRequest req;
                            req.input = text;
                            req.model = model;
                            req.dimensions = dimensions;

                            auto resp = provider->embed(req, timeouts);

                            total_input_tokens.fetch_add(resp.input_tokens, std::memory_order_relaxed);
                            total_api_calls.fetch_add(1, std::memory_order_relaxed);

                            if (cache_ttl > 0 && !resp.embedding.empty())
                            {
                                auto entry = std::make_shared<LLMCacheEntry>();
                                entry->result = serializeEmbedding(resp.embedding);
                                entry->function_name = name;
                                entry->model = model;
                                entry->result_size_bytes = entry->result.size();
                                entry->created_at = std::chrono::system_clock::now();
                                entry->expires_at = entry->created_at + std::chrono::seconds(cache_ttl);
                                cache.set(dk, entry);
                            }

                            {
                                std::lock_guard lock(results_mutex);
                                results[dk] = std::move(resp.embedding);
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
                                results[dk] = {};
                                return;
                            }
                            throw;
                        }
                    }
                });
            }

            pool->wait();
        }

        ProfileEvents::increment(ProfileEvents::LLMCacheHits, cache_hits);
        ProfileEvents::increment(ProfileEvents::LLMCacheMisses, cache_misses);
        ProfileEvents::increment(ProfileEvents::LLMAPICalls, total_api_calls.load());
        ProfileEvents::increment(ProfileEvents::LLMInputTokens, total_input_tokens.load());

        auto data_col = ColumnVector<Float32>::create();
        auto offsets_col = ColumnArray::ColumnOffsets::create();
        auto & data_vec = data_col->getData();
        auto & offsets_vec = offsets_col->getData();
        offsets_vec.reserve(input_rows_count);

        UInt64 rows_processed_count = 0;
        UInt64 rows_skipped_count = 0;
        size_t current_offset = 0;

        std::vector<const std::vector<Float32> *> ordered(input_rows_count, nullptr);

        for (const auto & [key, rows] : dedup_map)
        {
            auto it = results.find(key);
            const std::vector<Float32> * vec = nullptr;
            if (it != results.end() && !it->second.empty())
                vec = &it->second;

            for (size_t row : rows)
                ordered[row] = vec;
        }

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            if (ordered[i])
            {
                const auto & vec = *ordered[i];
                data_vec.insert(data_vec.end(), vec.begin(), vec.end());
                current_offset += vec.size();
                ++rows_processed_count;
            }
            else
            {
                ++rows_skipped_count;
            }
            offsets_vec.push_back(current_offset);
        }

        ProfileEvents::increment(ProfileEvents::LLMRowsProcessed, rows_processed_count);
        ProfileEvents::increment(ProfileEvents::LLMRowsSkipped, rows_skipped_count);

        return ColumnArray::create(std::move(data_col), std::move(offsets_col));
    }

private:
    ContextPtr context;
};

}

REGISTER_FUNCTION(LLMGenerateEmbedding)
{
    factory.registerFunction<FunctionLLMGenerateEmbedding>(FunctionDocumentation{
        .description = "Generates an embedding vector for the given text using an LLM embedding model.",
        .syntax = "LLMGenerateEmbedding([collection,] text, dimensions)",
        .arguments = {{"text", "Input text to embed"}, {"dimensions", "Dimensionality of the output embedding vector"}},
        .returned_value = {"Embedding vector as Array(Float32).", {"Array(Float32)"}},
        .examples = {{"basic", "SELECT LLMGenerateEmbedding('Hello world', 256)", ""}},
        .category = FunctionDocumentation::Category::Other});
}

}
