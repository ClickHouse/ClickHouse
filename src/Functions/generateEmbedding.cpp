#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/AI/IAIProvider.h>
#include <Functions/AI/AIResultCache.h>
#include <Functions/AI/AIQuotaTracker.h>

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

#include <IO/ConnectionTimeouts.h>
#include <Core/Settings.h>
#include <Core/ServerSettings.h>
#include <Interpreters/Context.h>

#include <unordered_map>
#include <unordered_set>

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
    extern const SettingsUInt64 generateembedding_max_batch_size;
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
    chassert(data.size() % sizeof(Float32) == 0);
    size_t count = data.size() / sizeof(Float32);
    std::vector<Float32> vec(count);
    memcpy(vec.data(), data.data(), data.size());
    return vec;
}

bool looksLikeURL(const String & s)
{
    return s.starts_with("http://") || s.starts_with("https://");
}

/// or_null=false: generateEmbedding        -> throws on errors
/// or_null=true:  generateEmbeddingOrNull  -> returns empty array [] on errors instead of throwing
///
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

    explicit FunctionGenerateEmbeddingImpl(ContextPtr context_)
        : ai_request_timeout_sec(context_->getSettingsRef()[Setting::ai_request_timeout_sec])
        , ai_max_concurrent_requests(context_->getSettingsRef()[Setting::ai_max_concurrent_requests])
        , ai_max_retries(context_->getSettingsRef()[Setting::ai_max_retries])
        , ai_retry_initial_delay_ms(context_->getSettingsRef()[Setting::ai_retry_initial_delay_ms])
        , ai_cache_ttl_sec(context_->getSettingsRef()[Setting::ai_cache_ttl_sec])
        , ai_max_rows_per_query(context_->getSettingsRef()[Setting::ai_max_rows_per_query])
        , ai_max_input_tokens_per_query(context_->getSettingsRef()[Setting::ai_max_input_tokens_per_query])
        , ai_max_output_tokens_per_query(context_->getSettingsRef()[Setting::ai_max_output_tokens_per_query])
        , ai_max_api_calls_per_query(context_->getSettingsRef()[Setting::ai_max_api_calls_per_query])
        , ai_on_error(context_->getSettingsRef()[Setting::ai_on_error])
        , ai_on_quota_exceeded(context_->getSettingsRef()[Setting::ai_on_quota_exceeded])
        , max_batch_size(context_->getSettingsRef()[Setting::generateembedding_max_batch_size] ? 0 : DEFAULT_EMBED_BATCH_SIZE)
        , default_ai_provider(context_->getSettingsRef()[Setting::default_ai_provider])
        , settings(context_->getSettingsRef())
        , server_settings(context_->getServerSettings())
    {
    }

    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isStateful() const override { return true; }
    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return false; }
    bool useDefaultImplementationForNulls() const override { return false; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() < 2 || arguments.size() > 3)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} requires 2-3 arguments: [collection_or_url,] text, dimensions", name);

        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat32>());
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
    {
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
                String collection_name = first_arg.empty() ? default_ai_provider : first_arg;
                if (collection_name.empty())
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "No AI named collection specified and default_ai_provider is not set");

                const auto & nc = NamedCollectionFactory::instance().get(collection_name);
                provider_name = nc->getOrDefault<String>("provider", "openai");
                endpoint_val = nc->getOrDefault<String>("endpoint", "");
                model = nc->getOrDefault<String>("model", "");
                api_key = nc->getOrDefault<String>("api_key", "");
            }
        }
        else
        {
            String collection_name = default_ai_provider;
            if (collection_name.empty())
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "No AI named collection specified and default_ai_provider is not set");

            const auto & nc = NamedCollectionFactory::instance().get(collection_name);
            provider_name = nc->getOrDefault<String>("provider", "openai");
            endpoint_val = nc->getOrDefault<String>("endpoint", "");
            model = nc->getOrDefault<String>("model", "");
            api_key = nc->getOrDefault<String>("api_key", "");
        }

        if (endpoint_val.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "AI embedding endpoint is not configured");

        auto ai_provider = createAIProvider(provider_name, endpoint_val, api_key);

        const auto * dim_const = checkAndGetColumn<ColumnConst>(arguments[dim_arg_idx].column.get());
        if (!dim_const)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Dimensions argument must be a constant");
        UInt64 dimensions = dim_const->getUInt(0);

        String on_error = or_null ? "null" : String(ai_on_error);
        String on_quota = or_null ? "null" : String(ai_on_quota_exceeded);

        auto quota = std::make_shared<AIQuotaTracker>(
            ai_max_rows_per_query,
            ai_max_input_tokens_per_query,
            ai_max_output_tokens_per_query,
            ai_max_api_calls_per_query,
            on_quota,
            on_error);

        auto timeouts = ConnectionTimeouts::getHTTPTimeouts(settings, server_settings);
        timeouts.receive_timeout = Poco::Timespan(static_cast<int64_t>(ai_request_timeout_sec), 0);

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
            UInt128 key = AIResultCache::buildKey(name, model, 0, cache_args);
            dedup_map[key].push_back(i);
        }

        std::unordered_map<UInt128, std::vector<Float32>, UInt128Hash> results;
        std::mutex results_mutex;

        UInt64 total_api_calls = 0;
        UInt64 total_input_tokens = 0;
        UInt64 cache_hits = 0;
        UInt64 cache_misses = 0;

        auto & cache = AIResultCache::instance();

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
                ai_max_concurrent_requests, ai_max_concurrent_requests, ai_max_concurrent_requests);

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
                    for (UInt64 attempt = 0; attempt <= ai_max_retries; ++attempt)
                    {
                        try
                        {
                            AIEmbeddingRequest req;
                            req.model = model;
                            req.dimensions = dimensions;
                            req.inputs.reserve(batch_items.size());
                            for (const auto * item : batch_items)
                                req.inputs.push_back(item->text);

                            auto resp = ai_provider->embed(req, timeouts);

                            total_input_tokens += resp.input_tokens;
                            total_api_calls += 1;

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
                                            "AI embedding provider returned empty response for batch of {} inputs", batch_items.size());
                                }

                                for (size_t i = 0; i < batch_items.size(); ++i)
                                {
                                    const auto & embedding = (i < resp.embeddings.size()) ? resp.embeddings[i] : resp.embeddings.back();

                                    if (ai_cache_ttl_sec > 0 && !embedding.empty())
                                    {
                                        auto entry = std::make_shared<AICacheEntry>();
                                        entry->result = serializeEmbedding(embedding);
                                        entry->function_name = name;
                                        entry->model = model;
                                        entry->result_size_bytes = entry->result.size();
                                        entry->created_at = std::chrono::system_clock::now();
                                        entry->expires_at = entry->created_at + std::chrono::seconds(ai_cache_ttl_sec);
                                        cache.set(batch_items[i]->key, entry);
                                    }

                                    results[batch_items[i]->key] = std::move(embedding);
                                }
                            }
                            return;
                        }
                        catch (const Exception & e)
                        {
                            if (attempt < ai_max_retries && e.code() == ErrorCodes::RECEIVED_ERROR_FROM_REMOTE_IO_SERVER)
                            {
                                std::this_thread::sleep_for(std::chrono::milliseconds(ai_retry_initial_delay_ms * (1ULL << attempt)));
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
        ProfileEvents::increment(ProfileEvents::AIAPICalls, total_api_calls);
        ProfileEvents::increment(ProfileEvents::AIInputTokens, total_input_tokens);

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
    const UInt64 ai_request_timeout_sec;
    const UInt64 ai_max_concurrent_requests;
    const UInt64 ai_max_retries;
    const UInt64 ai_retry_initial_delay_ms;
    const UInt64 ai_cache_ttl_sec;
    const UInt64 ai_max_rows_per_query;
    const UInt64 ai_max_input_tokens_per_query;
    const UInt64 ai_max_output_tokens_per_query;
    const UInt64 ai_max_api_calls_per_query;
    const String ai_on_error;
    const String ai_on_quota_exceeded;
    const UInt64 max_batch_size;
    const String default_ai_provider;
    const Settings & settings;
    const ServerSettings & server_settings;
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
