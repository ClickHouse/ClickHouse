#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionBaseAI.h>
#include <Functions/AI/IAIProvider.h>
#include <Functions/AI/AIQuotaTracker.h>

#include <Common/ProfileEvents.h>
#include <Common/Exception.h>
#include <Common/VectorWithMemoryTracking.h>

#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>

#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>

#include <IO/ConnectionTimeouts.h>
#include <Core/Settings.h>
#include <Core/ServerSettings.h>
#include <Interpreters/Context.h>

#include <algorithm>
#include <cmath>
#include <limits>
#include <string_view>

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
    extern const SettingsUInt64 ai_function_retry_initial_delay_ms;
    extern const SettingsBool ai_function_throw_on_error;
    extern const SettingsNonZeroUInt64 ai_function_embedding_max_batch_size;
    extern const SettingsString ai_function_embedding_default_credentials;
}

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

namespace
{

/// Cosine similarity of two equal-length vectors. `1` means identical direction, `0` orthogonal,
/// `-1` opposite. Sets `is_null` and returns `0` when a vector has zero magnitude (cosine undefined).
Float32 cosineSimilarity(
    const VectorWithMemoryTracking<Float32> & a, const VectorWithMemoryTracking<Float32> & b, UInt8 & is_null)
{
    Float64 dot = 0;
    Float64 norm_a = 0;
    Float64 norm_b = 0;
    for (size_t i = 0; i < a.size(); ++i)
    {
        dot += static_cast<Float64>(a[i]) * static_cast<Float64>(b[i]);
        norm_a += static_cast<Float64>(a[i]) * static_cast<Float64>(a[i]);
        norm_b += static_cast<Float64>(b[i]) * static_cast<Float64>(b[i]);
    }

    if (norm_a == 0.0 || norm_b == 0.0)
    {
        is_null = 1;
        return 0;
    }

    Float64 cosine = dot / (std::sqrt(norm_a) * std::sqrt(norm_b));
    cosine = std::clamp(cosine, -1.0, 1.0); /// Guard against floating-point drift outside the valid range.
    return static_cast<Float32>(cosine);
}

class FunctionAiSimilarity final : public IFunction
{
public:
    static constexpr auto name = "aiSimilarity";

    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionAiSimilarity>(context); }

    explicit FunctionAiSimilarity(ContextPtr context_) : context(context_) {}

    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    /// func has side effects, f.e. each call updates quota, makes potentially expensive outside call, etc.
    bool isStateful() const override { return true; }

    /// Like `aiEmbed`, folding calls with identical args together is preferable.
    bool isDeterministic() const override { return true; }
    bool isDeterministicInScopeOfQuery() const override { return true; }

    bool isSuitableForConstantFolding() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }

    /// Handle Nullable cols explicitly, since setting this to true may call func with arbitrary input values
    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return false; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors mandatory_args{
            {"text1", static_cast<FunctionArgumentDescriptor::TypeValidator>(&FunctionBaseAI::isStringOrNullableString), nullptr, "String"},
            {"text2", static_cast<FunctionArgumentDescriptor::TypeValidator>(&FunctionBaseAI::isStringOrNullableString), nullptr, "String"},
            {"model", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), &isColumnConst, "const String"},
        };
        FunctionArgumentDescriptors optional_args{
            {"params", static_cast<FunctionArgumentDescriptor::TypeValidator>(&FunctionBaseAI::isStringToStringMap), &isColumnConst, "const Map(String, String)"},
        };
        validateFunctionArguments(*this, arguments, mandatory_args, optional_args);

        /// Always Nullable. The score is NULL when an operand is NULL/empty, or its embedding could not be computed.
        return makeNullable(std::make_shared<DataTypeFloat32>());
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const auto & settings = getContext()->getSettingsRef();
        auto params = FunctionBaseAI::resolveAIParams(
            getContext(), arguments, FunctionBaseAI::embeddingParams(), settings[Setting::ai_function_embedding_default_credentials]);

        UInt64 dimensions = params.getUInt("dimensions");
        String model(arguments[model_arg_index].column->getDataAt(0));

        auto provider = createAIProvider(
            params.collection.provider, params.collection.endpoint, params.collection.api_key, params.collection.api_version);
        if (!provider->supportsEmbeddings())
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                "AI provider '{}' does not support embeddings", params.collection.provider);

        if (input_rows_count == 0)
            return result_type->createColumn();

        UInt64 max_retries = settings[Setting::ai_function_max_retries].value;
        UInt64 retry_delay_ms = settings[Setting::ai_function_retry_initial_delay_ms].value;
        bool throw_on_error = settings[Setting::ai_function_throw_on_error].value;
        size_t max_batch_size = static_cast<size_t>(settings[Setting::ai_function_embedding_max_batch_size].value);

        /// Shared across every AI function call in the query
        auto quota_tracker = getContext()->getAIQuotaTracker();

        auto timeouts = ConnectionTimeouts::getHTTPTimeouts(settings, getContext()->getServerSettings());
        timeouts.receive_timeout = Poco::Timespan(static_cast<int64_t>(settings[Setting::ai_function_request_timeout_sec].value) /*s*/, 0 /*us*/);

        /// `isNullAt` and `getDataAt` are virtual on `IColumn`, so a single path covers `ColumnString`,
        /// `ColumnConst(ColumnString)`, `ColumnNullable` and `ColumnConst(ColumnNullable)`. A constant
        /// operand is read at index 0 instead of being materialized into `input_rows_count` rows.
        const IColumn & text1_column = *arguments[text1_arg_index].column;
        const IColumn & text2_column = *arguments[text2_arg_index].column;

        /// A row's operand contributes an embedding only when it is non-null and non-empty. The `isNullAt`
        /// check also guards the only case in which `ColumnNullable::getDataAt` throws (a NULL value).
        auto get_value = [](const IColumn & column, size_t row, std::string_view & out) -> bool
        {
            if (column.isNullAt(row))
                return false;
            out = column.getDataAt(row);
            return !out.empty();
        };

        /// Collect the operands that need an embedding. `left`/`right` map each row's operands to an index
        /// into `inputs`, or `no_input` when the operand is not embedded.
        VectorWithMemoryTracking<std::string_view> inputs;
        inputs.reserve(2 * input_rows_count);
        constexpr size_t no_input = std::numeric_limits<size_t>::max();
        VectorWithMemoryTracking<size_t> left(input_rows_count, no_input);
        VectorWithMemoryTracking<size_t> right(input_rows_count, no_input);

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            /// Only embed a row's operands when both are non-null and non-empty. If either side is
            /// missing, the row scores NULL regardless, so embedding the other side would waste an API
            /// call and quota.
            std::string_view text1;
            std::string_view text2;
            if (get_value(text1_column, i, text1) && get_value(text2_column, i, text2))
            {
                left[i] = inputs.size();
                inputs.push_back(text1);
                right[i] = inputs.size();
                inputs.push_back(text2);
            }
        }

        FunctionBaseAI::EmbeddingResult embedding_result;
        FunctionBaseAI::embedTexts(
            *provider, model, dimensions, getName(), inputs, max_batch_size, max_retries, retry_delay_ms, throw_on_error, *quota_tracker,
            timeouts, embedding_result);

        const auto & embeddings = embedding_result.embeddings;

        /// The simplest implementation would be a wrapper that embeds both operands and reuses the
        /// built-in `cosineDistance`. We deliberately do not, because feeding `cosineDistance`
        /// means materializing two full `Array(Float32)` columns holding one copy of every embedding
        /// per row. On a default-size block at 3072-dimensional embeddings that is ~1.5 GiB of
        /// temporaries. Instead we compute the cosine directly over the embedding vectors below,
        /// so the only large allocation is the score column itself.
        auto score_col = ColumnFloat32::create();
        auto null_map_col = ColumnUInt8::create(input_rows_count, static_cast<UInt8>(0));
        auto & scores = score_col->getData();
        auto & null_map = null_map_col->getData();
        scores.resize(input_rows_count);

        UInt64 rows_processed = 0; /// rows that received a (non-NULL) similarity score
        UInt64 rows_skipped = 0;   /// rows that are NULL because a needed embedding was skipped (quota/error)

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const size_t a = left[i];
            const size_t b = right[i];

            /// A row scores NULL when either operand had no text, an embedding it needs was skipped
            /// (quota/error), or the two vectors are not size-comparable (sanity check: both embeddings
            /// come from the same model, so their sizes should always match).
            if (a == no_input || b == no_input || embeddings[a].empty() || embeddings[b].empty()
                || embeddings[a].size() != embeddings[b].size())
            {
                scores[i] = 0;
                null_map[i] = 1;
                if (a != no_input && b != no_input && (embeddings[a].empty() || embeddings[b].empty()))
                    ++rows_skipped;
                continue;
            }

            /// `cosineSimilarity` sets `null_map[i]` for a zero-magnitude vector (cosine undefined).
            scores[i] = cosineSimilarity(embeddings[a], embeddings[b], null_map[i]);
            if (!null_map[i])
                ++rows_processed;
        }

        ProfileEvents::increment(ProfileEvents::AIRowsProcessed, rows_processed);
        ProfileEvents::increment(ProfileEvents::AIRowsSkipped, rows_skipped);

        return ColumnNullable::create(std::move(score_col), std::move(null_map_col));
    }

private:
    static constexpr size_t text1_arg_index = 0;
    static constexpr size_t text2_arg_index = 1;
    static constexpr size_t model_arg_index = 2;

    ContextPtr context;
    ContextPtr getContext() const { return context; }
};

}

REGISTER_FUNCTION(AiSimilarity)
{
    factory.registerFunction<FunctionAiSimilarity>(FunctionDocumentation{
        .description = R"(
Computes the semantic similarity of two texts using the configured embedding provider.

Calculates the vector embeddings of both texts and returns their
[cosine similarity](https://en.wikipedia.org/wiki/Cosine_similarity). A score of `-1` is given to
opposite embedding vectors, semantically this means texts with scores approaching `-1` are opposite in
meaning. A score of `0` means the vectors are orthogonal: semantically unrelated. Finally, a score of `1`
means the embedding vectors are pointing in the same direction, texts with scores approaching `1` are
similar in meaning. This is the complement of `cosineDistance` over the same embeddings
(`aiSimilarity = 1 - cosineDistance(embedding1, embedding2)`).

Batching, credentials, and the `dimensions` parameter match `aiEmbed`, including the
`ai_function_embedding_default_credentials` default-credentials setting.

Like `aiEmbed`, `model` is a required positional argument (a constant `String`), not read from the
named collection or the parameter map.
)",
        .syntax = "aiSimilarity(text1, text2, model[, params])",
        .arguments
        = {{"text1", "First text.", {"String"}},
           {"text2", "Second text.", {"String"}},
           {"model", "Embedding model name.", {"const String"}},
           {"params", "Optional constant `Map(String, String)` of parameters. Function-specific key: `dimensions` (target dimensionality of the embeddings; `0` or omitted means the model's native size). The common parameter `credentials` also applies (see [AI Functions](/reference/functions/regular-functions/ai-functions)).", {"Map(String, String)"}}},
        .returned_value = {"The cosine similarity in `[-1, 1]`, or NULL if either text is NULL or empty, an embedding request failed and `ai_function_throw_on_error` is disabled, or a quota was exceeded with `ai_function_throw_on_quota_exceeded` disabled.", {"Nullable(Float32)"}},
        .examples
        = {{"Compare two strings (`credentials` can be omitted if the `ai_function_embedding_default_credentials` setting is set)", "SELECT aiSimilarity('cat', 'kitten', 'text-embedding-3-small', map('credentials', 'ai_embedding_credentials'))", ""},
           {"Rank reviews by similarity to a query", "CREATE TABLE product_reviews (review String) ENGINE = Memory;\nINSERT INTO product_reviews VALUES ('It works well under rain.');\nSELECT review FROM product_reviews ORDER BY aiSimilarity(review, 'It works well under rain', 'text-embedding-3-small') DESC LIMIT 100", ""},
           {"Semantic dedup over a self-join", "CREATE TABLE docs (id UInt64, title String) ENGINE = Memory;\nINSERT INTO docs VALUES (1, 'ClickHouse documentation'), (2, 'ClickHouse database guide');\nSELECT a.id, b.id FROM docs a, docs b WHERE a.id < b.id AND aiSimilarity(a.title, b.title, 'text-embedding-3-small') > 0.9", ""}},
        .introduced_in = {26, 8},
        .category = FunctionDocumentation::Category::AI});

    factory.registerAlias("AISimilarity", "aiSimilarity");
}

}
