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
    extern const Event AIInputTokens;
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
    extern const SettingsUInt64 embedding_max_batch_size;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int RECEIVED_ERROR_FROM_REMOTE_IO_SERVER;
    extern const int SUPPORT_IS_DISABLED;
}

namespace
{

constexpr size_t DEFAULT_EMBED_BATCH_SIZE = 100;

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

bool looksLikeURL(const String & s)
{
    return s.starts_with("http://") || s.starts_with("https://");
}

/// or_null=false: generateEmbedding       -> throws on API errors
/// or_null=true:  generateEmbeddingOrNull  -> returns empty array [] on errors instead of throwing
/// Both return Array(Float32). Nullable(Array) is not supported in ClickHouse.
template <bool or_null>
class FunctionGenerateEmbeddingImpl final : public IFunction
{
public:
    static constexpr auto name = or_null ? "generateEmbeddingOrNull" : "generateEmbedding";

    static FunctionPtr create(ContextPtr context)
    {
        if (!context->getSettingsRef()[Setting::allow_experimental_ai_functions])
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                "AI function '{}' is experimental. Set `allow_experimental_ai_functions` setting to enable", name);
        return std::make_shared<FunctionGenerateEmbeddingImpl>(context);
    }
    explicit FunctionGenerateEmbeddingImpl(ContextPtr context_) : context(context_) {}

    ContextPtr context;

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
                "Function {} requires 2-3 arguments: [collection_or_url,] text, dimensions", name);
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat32>());
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
    {
        const auto & settings = context->getSettingsRef();

        String provider_name;
        String endpoint_val;
        String model;
        String api_key;

        size_t text_arg_idx = 0;
        size_t dim_arg_idx = 1;

        if (arguments.size() == 3)
        {
            text_arg_idx = 1;
            dim_arg_idx = 2;

            const auto * first_const = checkAndGetColumn<ColumnConst>(arguments[0].column.get());
            if (!first_const)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "First argument of {} must be a constant string (named collection or URL) when 3 arguments are provided", name);
            String first_arg(first_const->getDataAt(0));

            if (!first_arg.empty() && looksLikeURL(first_arg))
            {
                provider_name = "openai";
                endpoint_val = first_arg;
            }
            else
            {
                String collection_name = first_arg.empty() ? String(settings[Setting::default_llm_resource].value) : first_arg;
                if (collection_name.empty())
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "No LLM named collection specified and default_llm_resource is not set");

                const auto & nc = NamedCollectionFactory::instance().get(collection_name);
                provider_name = nc->getOrDefault<String>("provider", "openai");
                endpoint_val = nc->getOrDefault<String>("endpoint", "");
                model = nc->getOrDefault<String>("model", "");
                api_key = nc->getOrDefault<String>("api_key", "");
            }
        }
        else
        {
            String collection_name = settings[Setting::default_llm_resource].value;
            if (collection_name.empty())
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "No LLM named collection specified and default_llm_resource is not set");

            const auto & nc = NamedCollectionFactory::instance().get(collection_name);
            provider_name = nc->getOrDefault<String>("provider", "openai");
            endpoint_val = nc->getOrDefault<String>("endpoint", "");
            model = nc->getOrDefault<String>("model", "");
            api_key = nc->getOrDefault<String>("api_key", "");
        }

        if (endpoint_val.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "LLM embedding endpoint is not configured");

        size_t max_batch_size = settings[Setting::embedding_max_batch_size].value;
        if (max_batch_size == 0)
            max_batch_size = DEFAULT_EMBED_BATCH_SIZE;

        auto provider = createLLMProvider(provider_name, endpoint_val, api_key);

        const auto * dim_const = checkAndGetColumn<ColumnConst>(arguments[dim_arg_idx].column.get());
        if (!dim_const)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Dimensions argument must be a constant");
        UInt64 dimensions = dim_const->getUInt(0);

        UInt64 timeout_sec = settings[Setting::llm_request_timeout_sec].value;
        UInt64 max_concurrent = settings[Setting::llm_max_concurrent_requests].value;
        UInt64 max_retries = settings[Setting::llm_max_retries].value;
        UInt64 retry_delay_ms = settings[Setting::llm_retry_initial_delay_ms].value;
        UInt64 cache_ttl = settings[Setting::llm_cache_ttl_sec].value;

        String on_error = or_null ? "null" : String(settings[Setting::llm_on_error].value);
        String on_quota = or_null ? "null" : String(settings[Setting::llm_on_quota_exceeded].value);

        auto quota = std::make_shared<LLMQuotaTracker>(
            settings[Setting::llm_max_rows_per_query].value,
            settings[Setting::llm_max_input_tokens_per_query].value,
            settings[Setting::llm_max_output_tokens_per_query].value,
            settings[Setting::llm_max_api_calls_per_query].value,
            on_quota,
            on_error);

        auto timeouts = ConnectionTimeouts::getHTTPTimeouts(settings, context->getServerSettings());
        timeouts.receive_timeout = Poco::Timespan(static_cast<int64_t>(timeout_sec), 0);

        std::unordered_map<UInt128, std::vector<size_t>, UInt128Hash> dedup_map;
        std::unordered_set<size_t> null_input_rows;
        std::unordered_set<size_t> empty_input_rows;

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const auto & text_col = arguments[text_arg_idx].column;
            if (text_col->isNullAt(i))
            {
                null_input_rows.insert(i);
                continue;
            }

            String text(text_col->getDataAt(i));
            if (text.empty())
            {
                empty_input_rows.insert(i);
                continue;
            }

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

        struct DispatchItem
        {
            UInt128 key;
            String text;
        };
        std::vector<DispatchItem> to_dispatch;

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
                String text(arguments[text_arg_idx].column->getDataAt(rows[0]));
                to_dispatch.push_back({key, std::move(text)});
                ++cache_misses;
            }
        }

        if (!to_dispatch.empty())
        {
            std::vector<std::vector<DispatchItem *>> batches;
            for (size_t i = 0; i < to_dispatch.size(); i += max_batch_size)
            {
                batches.emplace_back();
                auto & batch = batches.back();
                for (size_t j = i; j < std::min(i + max_batch_size, to_dispatch.size()); ++j)
                    batch.push_back(&to_dispatch[j]);
            }

            auto pool = std::make_unique<ThreadPool>(
                CurrentMetrics::end(), CurrentMetrics::end(), CurrentMetrics::end(),
                max_concurrent, max_concurrent, max_concurrent);

            for (auto & batch : batches)
            {
                UInt64 total_rows_for_batch = 0;
                for (auto * item : batch)
                    total_rows_for_batch += dedup_map[item->key].size();

                if (!quota->checkBeforeDispatch(0, total_rows_for_batch))
                {
                    std::lock_guard lock(results_mutex);
                    for (auto * item : batch)
                        results[item->key] = {};
                    continue;
                }

                pool->scheduleOrThrowOnError([&, batch_items = std::move(batch)]
                {
                    for (UInt64 attempt = 0; attempt <= max_retries; ++attempt)
                    {
                        try
                        {
                            LLMEmbeddingRequest req;
                            req.model = model;
                            req.dimensions = dimensions;
                            req.inputs.reserve(batch_items.size());
                            for (const auto * item : batch_items)
                                req.inputs.push_back(item->text);

                            auto resp = provider->embed(req, timeouts);

                            total_input_tokens.fetch_add(resp.input_tokens, std::memory_order_relaxed);
                            total_api_calls.fetch_add(1, std::memory_order_relaxed);

                            {
                                std::lock_guard lock(results_mutex);
                                if (resp.embeddings.empty())
                                {
                                    if constexpr (or_null)
                                    {
                                        for (const auto * item : batch_items)
                                            results[item->key] = {};
                                        return;
                                    }
                                    else
                                        throw Exception(ErrorCodes::RECEIVED_ERROR_FROM_REMOTE_IO_SERVER,
                                            "LLM embedding provider returned empty response for batch of {} inputs", batch_items.size());
                                }

                                for (size_t i = 0; i < batch_items.size(); ++i)
                                {
                                    const auto & embedding = (i < resp.embeddings.size()) ? resp.embeddings[i] : resp.embeddings.back();

                                    if (cache_ttl > 0 && !embedding.empty())
                                    {
                                        auto entry = std::make_shared<LLMCacheEntry>();
                                        entry->result = serializeEmbedding(embedding);
                                        entry->function_name = name;
                                        entry->model = model;
                                        entry->result_size_bytes = entry->result.size();
                                        entry->created_at = std::chrono::system_clock::now();
                                        entry->expires_at = entry->created_at + std::chrono::seconds(cache_ttl);
                                        cache.set(batch_items[i]->key, entry);
                                    }

                                    results[batch_items[i]->key] = std::move(embedding);
                                }
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

                            if constexpr (or_null)
                            {
                                std::lock_guard lock(results_mutex);
                                for (const auto * item : batch_items)
                                    results[item->key] = {};
                                return;
                            }
                            else
                            {
                                if (quota->handleRowError())
                                {
                                    std::lock_guard lock(results_mutex);
                                    for (const auto * item : batch_items)
                                        results[item->key] = {};
                                    return;
                                }
                                throw;
                            }
                        }
                        catch (...)
                        {
                            if constexpr (or_null)
                            {
                                std::lock_guard lock(results_mutex);
                                for (const auto * item : batch_items)
                                    results[item->key] = {};
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

        ProfileEvents::increment(ProfileEvents::AIRowsProcessed, rows_processed_count);
        ProfileEvents::increment(ProfileEvents::AIRowsSkipped, rows_skipped_count);

        return ColumnArray::create(std::move(data_col), std::move(offsets_col));
    }

private:
};

}

REGISTER_FUNCTION(GenerateEmbedding)
{
    factory.registerFunction<FunctionGenerateEmbeddingImpl<false>>(FunctionDocumentation{
        .description = "Generates embedding vectors for the given text using an embedding model. "
                       "Supports batch API calls for efficient processing of multiple rows. "
                       "Throws on API errors. Returns an empty array for NULL or empty inputs.",
        .syntax = "generateEmbedding([collection_or_url,] text, dimensions)",
        .arguments = {
            {"collection_or_url", "Optional named collection name or inline URL (e.g. 'http://localhost:8080')"},
            {"text", "Input text to embed"},
            {"dimensions", "Dimensionality of the output embedding vector (must be constant)"}},
        .returned_value = {"Embedding vector as Array(Float32). Empty array for NULL/empty inputs.", {"Array(Float32)"}},
        .examples = {
            {"basic", "SELECT generateEmbedding('Hello world', 256)", ""}},
        .introduced_in = {26, 4},
        .category = FunctionDocumentation::Category::AI});
}

REGISTER_FUNCTION(GenerateEmbeddingOrNull)
{
    factory.registerFunction<FunctionGenerateEmbeddingImpl<true>>(FunctionDocumentation{
        .description = "Generates embedding vectors for the given text using an embedding model. "
                       "Returns an empty array instead of throwing on API errors. "
                       "Supports batch API calls for efficient processing of multiple rows.",
        .syntax = "generateEmbeddingOrNull([collection_or_url,] text, dimensions)",
        .arguments = {
            {"collection_or_url", "Optional named collection name or inline URL (e.g. 'http://localhost:8080')"},
            {"text", "Input text to embed"},
            {"dimensions", "Dimensionality of the output embedding vector (must be constant)"}},
        .returned_value = {"Embedding vector as Array(Float32). Empty array for NULL/empty inputs or on API failure.", {"Array(Float32)"}},
        .examples = {
            {"basic", "SELECT generateEmbeddingOrNull('Hello world', 256)", ""},
            {"error_safe", "SELECT generateEmbeddingOrNull(text, 256) FROM docs", ""}},
        .introduced_in = {26, 4},
        .category = FunctionDocumentation::Category::AI});
}

}
