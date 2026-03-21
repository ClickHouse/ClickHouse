#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/SipHash.h>
#include <Common/HashTable/Hash.h>
#include <base/extended_types.h>
#include <Common/CurrentMetrics.h>
#include <Common/ThreadPool.h>
#include <Common/logger_useful.h>
#include <Common/setThreadName.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/AIEmbed/EmbeddingCache.h>
#include <Functions/AIEmbed/EmbeddingConnection.h>
#include <Functions/AIEmbed/EmbeddingProvider.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>

#include <future>
#include <unordered_map>
#include <vector>


namespace CurrentMetrics
{
    extern const Metric AIEmbedThreads;
    extern const Metric AIEmbedThreadsActive;
    extern const Metric AIEmbedThreadsScheduled;
}

namespace ProfileEvents
{
    extern const Event AIEmbedRowsProcessed;
    extern const Event AIEmbedRequestsMade;
    extern const Event AIEmbedFailedRows;
    extern const Event AIEmbedCacheHits;
    extern const Event AIEmbedTotalBytes;
}

namespace DB
{

namespace Setting
{
    extern const SettingsBool allow_experimental_ai_functions;
    extern const SettingsUInt64 ai_embed_timeout_ms;
    extern const SettingsUInt64 ai_embed_max_retries;
    extern const SettingsUInt64 ai_embed_max_rows_per_query;
    extern const SettingsUInt64 ai_embed_max_parallel_requests;
    extern const SettingsUInt64 ai_embed_cache_max_entries;
    extern const SettingsUInt64 ai_embed_cache_max_bytes;
}

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
    extern const int SUPPORT_IS_DISABLED;
    extern const int TOO_MANY_ROWS_OR_BYTES;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

/// Compute cache key: SipHash128(model + '\0' + text)
UInt128 computeCacheKey(const String & model, std::string_view text)
{
    SipHash hash;
    hash.update(model.data(), model.size());
    hash.update("\0", 1);
    hash.update(text.data(), text.size());
    return hash.get128();
}

}


/// AI_EMBED(connection_name, model, input_text) → Array(Float32)
/// AI_EMBED(model, input_text) → Array(Float32)  [Cloud-only shortcut, currently disabled for OSS]
template <bool or_null>
class FunctionAIEmbedImpl final : public IFunction
{
private:
    ContextPtr context;
    mutable EmbeddingProviderPtr provider;
    mutable String cached_connection_name;

public:
    static constexpr auto name = or_null ? "AI_EMBED_OR_NULL" : "AI_EMBED";

    static FunctionPtr create(ContextPtr context_)
    {
        if (!context_->getSettingsRef()[Setting::allow_experimental_ai_functions])
            throw Exception(
                ErrorCodes::SUPPORT_IS_DISABLED,
                "Function '{}' is experimental. Set `allow_experimental_ai_functions` setting to enable it",
                name);

        return std::make_shared<FunctionAIEmbedImpl>(context_->getGlobalContext());
    }

    explicit FunctionAIEmbedImpl(ContextPtr context_) : context(context_) {}

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForNulls() const override { return true; }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override
    {
        return {0, 1}; /// connection_name and model are always constant
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 3)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} requires 3 arguments: connection_name, model, input_text. Got {} arguments",
                getName(), arguments.size());

        /// Validate connection_name (arg 0) is a constant string
        if (!isString(arguments[0].type))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "First argument (connection_name) of function {} must be String, got {}",
                getName(), arguments[0].type->getName());

        if (!arguments[0].column || !isColumnConst(*arguments[0].column))
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "First argument (connection_name) of function {} must be a constant String",
                getName());

        /// Validate model (arg 1) is a constant string
        if (!isString(arguments[1].type))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Second argument (model) of function {} must be String, got {}",
                getName(), arguments[1].type->getName());

        if (!arguments[1].column || !isColumnConst(*arguments[1].column))
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Second argument (model) of function {} must be a constant String",
                getName());

        /// Validate input_text (arg 2) is String
        if (!isString(arguments[2].type))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Third argument (input_text) of function {} must be String, got {}",
                getName(), arguments[2].type->getName());

        /// Both variants return Array(Float32).
        /// AI_EMBED_OR_NULL returns empty array [] on failure instead of throwing.
        /// (ClickHouse does not support Nullable(Array(...)))
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat32>());
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
    {
        const auto & settings = context->getSettingsRef();

        /// Safety limit
        size_t max_rows = settings[Setting::ai_embed_max_rows_per_query];
        if (input_rows_count > max_rows)
            throw Exception(
                ErrorCodes::TOO_MANY_ROWS_OR_BYTES,
                "AI_EMBED: too many rows ({}). Limit is {} (setting `ai_embed_max_rows_per_query`). "
                "Increase the limit or use LIMIT to process fewer rows.",
                input_rows_count, max_rows);

        /// Extract constant args
        const auto * connection_col = checkAndGetColumnConst<ColumnString>(arguments[0].column.get());
        const auto * model_col = checkAndGetColumnConst<ColumnString>(arguments[1].column.get());

        if (!connection_col || !model_col)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "First two arguments of {} must be constant strings", getName());

        String connection_name = connection_col->getValue<String>();
        String model = model_col->getValue<String>();

        /// Resolve provider (cached for reuse)
        if (!provider || cached_connection_name != connection_name)
        {
            provider = resolveEmbeddingProvider(connection_name, context);
            cached_connection_name = connection_name;
        }

        /// Extract input texts
        const auto * input_col = checkAndGetColumn<ColumnString>(arguments[2].column.get());
        if (!input_col)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Third argument of {} must be a String column", getName());

        /// Settings
        size_t timeout_ms = settings[Setting::ai_embed_timeout_ms];
        size_t max_retries = settings[Setting::ai_embed_max_retries];
        size_t max_parallel = settings[Setting::ai_embed_max_parallel_requests];
        size_t cache_max_entries = settings[Setting::ai_embed_cache_max_entries];
        size_t cache_max_bytes = settings[Setting::ai_embed_cache_max_bytes];

        /// Update cache limits
        auto & cache = EmbeddingCache::instance();
        cache.updateLimits(cache_max_entries, cache_max_bytes);

        /// === DEDUP + CACHE LOOKUP ===
        /// Map unique texts to their first occurrence index
        std::unordered_map<UInt128, size_t, UInt128Hash> key_to_dedup_index;
        std::vector<UInt128> row_keys(input_rows_count);
        std::vector<size_t> row_to_dedup(input_rows_count);

        /// Results indexed by dedup index
        std::vector<std::vector<Float32>> dedup_results;
        std::vector<bool> dedup_has_result;

        /// Texts that need embedding (cache misses)
        std::vector<size_t> to_embed_dedup_indices;
        std::vector<std::string_view> to_embed_texts;

        size_t cache_hits = 0;

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            auto text = input_col->getDataAt(i);

            /// Skip empty texts — return empty array for them
            if (text.empty())
            {
                size_t dedup_idx = dedup_results.size();
                dedup_results.emplace_back(); /// empty vector
                dedup_has_result.push_back(true); /// mark as "has result" (empty array)
                row_to_dedup[i] = dedup_idx;
                continue;
            }

            UInt128 key = computeCacheKey(model, text);
            row_keys[i] = key;

            auto [it, inserted] = key_to_dedup_index.emplace(key, dedup_results.size());
            if (inserted)
            {
                /// First occurrence of this text
                size_t dedup_idx = dedup_results.size();
                dedup_results.emplace_back();
                dedup_has_result.push_back(false);
                row_to_dedup[i] = dedup_idx;

                /// Check cache
                auto cached = cache.get(key);
                if (cached)
                {
                    dedup_results[dedup_idx] = std::move(*cached);
                    dedup_has_result[dedup_idx] = true;
                    ++cache_hits;
                }
                else
                {
                    to_embed_dedup_indices.push_back(dedup_idx);
                    to_embed_texts.push_back(text);
                }
            }
            else
            {
                row_to_dedup[i] = it->second;
            }
        }

        ProfileEvents::increment(ProfileEvents::AIEmbedCacheHits, cache_hits);

        /// === ASYNC BATCH DISPATCH ===
        if (!to_embed_texts.empty())
        {
            /// Determine batch size from provider config
            auto config = resolveConnectionConfig(connection_name, context);
            size_t batch_size = config.max_batch_size;

            /// Split into sub-batches
            struct SubBatch
            {
                std::vector<std::string_view> texts;
                std::vector<size_t> dedup_indices;
            };

            std::vector<SubBatch> sub_batches;
            for (size_t i = 0; i < to_embed_texts.size(); i += batch_size)
            {
                SubBatch batch;
                size_t end = std::min(i + batch_size, to_embed_texts.size());
                batch.texts.assign(to_embed_texts.begin() + i, to_embed_texts.begin() + end);
                batch.dedup_indices.assign(to_embed_dedup_indices.begin() + i, to_embed_dedup_indices.begin() + end);
                sub_batches.push_back(std::move(batch));
            }

            size_t total_bytes = 0;
            for (const auto & text : to_embed_texts)
                total_bytes += text.size();
            ProfileEvents::increment(ProfileEvents::AIEmbedTotalBytes, total_bytes);

            /// Dispatch sub-batches concurrently
            if (sub_batches.size() == 1 || max_parallel <= 1)
            {
                /// Single-threaded path (common case: small batches)
                for (auto & batch : sub_batches)
                {
                    try
                    {
                        auto embeddings = provider->embed(model, batch.texts, timeout_ms, max_retries);
                        ProfileEvents::increment(ProfileEvents::AIEmbedRequestsMade, 1);

                        for (size_t j = 0; j < embeddings.size(); ++j)
                        {
                            size_t dedup_idx = batch.dedup_indices[j];
                            dedup_results[dedup_idx] = std::move(embeddings[j]);
                            dedup_has_result[dedup_idx] = true;

                            /// Insert into cache
                            UInt128 key = computeCacheKey(model, batch.texts[j]);
                            cache.put(key, dedup_results[dedup_idx]);
                        }
                    }
                    catch (...)
                    {
                        ProfileEvents::increment(ProfileEvents::AIEmbedRequestsMade, 1);

                        if constexpr (or_null)
                        {
                            ProfileEvents::increment(ProfileEvents::AIEmbedFailedRows, batch.texts.size());
                            LOG_WARNING(getLogger("FunctionAIEmbed"),
                                "Embedding batch failed (or_null mode), {} rows will be NULL: {}",
                                batch.texts.size(), getCurrentExceptionMessage(false));
                            /// Leave dedup_has_result[idx] = false → will be NULL
                        }
                        else
                        {
                            throw;
                        }
                    }
                }
            }
            else
            {
                /// Multi-threaded path
                ThreadPool pool(
                    CurrentMetrics::AIEmbedThreads,
                    CurrentMetrics::AIEmbedThreadsActive,
                    CurrentMetrics::AIEmbedThreadsScheduled,
                    std::min(max_parallel, sub_batches.size()));

                struct BatchResult
                {
                    std::vector<std::vector<Float32>> embeddings;
                    std::exception_ptr exception;
                };

                std::vector<std::future<BatchResult>> futures;
                futures.reserve(sub_batches.size());

                for (auto & batch : sub_batches)
                {
                    auto promise = std::make_shared<std::promise<BatchResult>>();
                    futures.push_back(promise->get_future());

                    pool.scheduleOrThrowOnError([
                        cur_provider = this->provider,
                        &model,
                        texts = batch.texts,
                        timeout_ms,
                        max_retries,
                        promise]()
                    {
                        BatchResult result;
                        try
                        {
                            result.embeddings = cur_provider->embed(model, texts, timeout_ms, max_retries);
                        }
                        catch (...)
                        {
                            result.exception = std::current_exception();
                        }
                        promise->set_value(std::move(result));
                    });
                }

                pool.wait();

                /// Collect results
                for (size_t batch_idx = 0; batch_idx < sub_batches.size(); ++batch_idx)
                {
                    auto result = futures[batch_idx].get();
                    ProfileEvents::increment(ProfileEvents::AIEmbedRequestsMade, 1);

                    if (result.exception)
                    {
                        if constexpr (or_null)
                        {
                            /// Mark all rows in this batch as failed
                            ProfileEvents::increment(ProfileEvents::AIEmbedFailedRows, sub_batches[batch_idx].texts.size());
                            continue; /// Leave dedup_has_result[idx] = false → will be NULL
                        }
                        else
                        {
                            std::rethrow_exception(result.exception);
                        }
                    }

                    auto & batch = sub_batches[batch_idx];
                    for (size_t j = 0; j < result.embeddings.size(); ++j)
                    {
                        size_t dedup_idx = batch.dedup_indices[j];
                        dedup_results[dedup_idx] = std::move(result.embeddings[j]);
                        dedup_has_result[dedup_idx] = true;

                        UInt128 key = computeCacheKey(model, batch.texts[j]);
                        cache.put(key, dedup_results[dedup_idx]);
                    }
                }
            }
        }

        ProfileEvents::increment(ProfileEvents::AIEmbedRowsProcessed, input_rows_count);

        /// === BUILD RESULT COLUMN ===
        /// Construct ColumnArray(ColumnFloat32) from dedup results + row index mapping
        auto float_column = ColumnFloat32::create();
        auto offsets_column = ColumnArray::ColumnOffsets::create();

        auto & float_data = float_column->getData();
        auto & offsets_data = offsets_column->getData();
        offsets_data.reserve(input_rows_count);

        size_t current_offset = 0;
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            size_t dedup_idx = row_to_dedup[i];
            if (dedup_has_result[dedup_idx])
            {
                const auto & embedding = dedup_results[dedup_idx];
                float_data.insert(embedding.begin(), embedding.end());
                current_offset += embedding.size();
            }
            /// else: no result (or_null failure / empty input) → empty array (offset unchanged)
            offsets_data.push_back(current_offset);
        }

        return ColumnArray::create(std::move(float_column), std::move(offsets_column));
    }
};


using FunctionAIEmbed = FunctionAIEmbedImpl<false>;
using FunctionAIEmbedOrNull = FunctionAIEmbedImpl<true>;


REGISTER_FUNCTION(AIEmbed)
{
    FunctionDocumentation::Description description = R"(
Generates vector embeddings from text using an external embedding API.

Connects to an embedding service (OpenAI-compatible or HuggingFace TEI) configured via a named collection
and returns the embedding as `Array(Float32)`. The result works directly with `cosineDistance`, `L2Distance`,
and vector similarity indices.

This function is experimental and must be enabled with `SET allow_experimental_ai_functions = 1`.
)";

    FunctionDocumentation::Syntax syntax = "AI_EMBED(connection_name, model, input_text)";
    FunctionDocumentation::Arguments arguments = {
        {"connection_name", "Name of a named collection containing provider configuration (endpoint, api_key, etc.), or an inline URL for HuggingFace TEI.", {"const String"}},
        {"model", "Embedding model name (e.g. 'text-embedding-3-small').", {"const String"}},
        {"input_text", "Text to embed.", {"String"}},
    };
    FunctionDocumentation::ReturnedValue returned_value = {"The embedding vector.", {"Array(Float32)"}};
    FunctionDocumentation::Examples examples = {
        {
            "Semantic search",
            "SELECT title, cosineDistance(embedding, AI_EMBED('my_conn', 'text-embedding-3-small', 'machine learning')) AS score FROM articles ORDER BY score ASC LIMIT 10",
            "",
        },
        {
            "Batch backfill",
            "INSERT INTO articles_with_embeddings SELECT *, AI_EMBED('my_conn', 'text-embedding-3-small', title) FROM articles",
            "",
        },
    };
    FunctionDocumentation::IntroducedIn introduced_in = {25, 12};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::MachineLearning;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionAIEmbed>(documentation);
}


REGISTER_FUNCTION(AIEmbedOrNull)
{
    FunctionDocumentation::Description description = R"(
Generates vector embeddings from text using an external embedding API.
Returns an empty array instead of throwing an exception on failure.

This is the graceful variant of `AI_EMBED`. On error, affected rows get an empty array `[]`
and a warning is logged. Use this for batch operations where partial failures are acceptable.

This function is experimental and must be enabled with `SET allow_experimental_ai_functions = 1`.
)";

    FunctionDocumentation::Syntax syntax = "AI_EMBED_OR_NULL(connection_name, model, input_text)";
    FunctionDocumentation::Arguments arguments = {
        {"connection_name", "Name of a named collection containing provider configuration.", {"const String"}},
        {"model", "Embedding model name.", {"const String"}},
        {"input_text", "Text to embed.", {"String"}},
    };
    FunctionDocumentation::ReturnedValue returned_value = {"The embedding vector, or empty array on failure.", {"Array(Float32)"}};
    FunctionDocumentation::Examples examples = {
        {
            "Graceful embedding",
            "SELECT id, AI_EMBED_OR_NULL('my_conn', 'model', text) AS emb FROM docs",
            "",
        },
    };
    FunctionDocumentation::IntroducedIn introduced_in = {25, 12};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::MachineLearning;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionAIEmbedOrNull>(documentation);
}

}
