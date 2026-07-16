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
#include <unordered_map>

namespace ProfileEvents
{
    extern const Event AIInputTokens;
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
    extern const SettingsNonZeroUInt64 ai_function_embedding_max_batch_size;
    extern const SettingsString ai_function_embedding_default_credentials;
}

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int SUPPORT_IS_DISABLED;
}

namespace
{

/// Cosine similarity of two equal-length vectors, in the `[-1, 1]` range. `1` means identical
/// direction, `0` orthogonal, `-1` opposite. Sets `is_null` and returns `0` when a vector has zero
/// magnitude (cosine undefined).
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

    explicit FunctionAiSimilarity(ContextPtr context_) : context(context_)
    {
        if (!getContext()->getSettingsRef()[Setting::allow_experimental_ai_functions])
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                "AI functions are experimental. Set `allow_experimental_ai_functions` setting to enable it");
    }

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
            /// `model` must be a plain (non-nullable) `String`; constness is enforced by the column validator.
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

        AIQuotaTracker quota(
            settings[Setting::ai_function_max_input_tokens_per_query].value,
            settings[Setting::ai_function_max_output_tokens_per_query].value,
            settings[Setting::ai_function_max_api_calls_per_query].value,
            settings[Setting::ai_function_throw_on_quota_exceeded].value);

        auto timeouts = ConnectionTimeouts::getHTTPTimeouts(settings, getContext()->getServerSettings());
        timeouts.receive_timeout = Poco::Timespan(static_cast<int64_t>(settings[Setting::ai_function_request_timeout_sec].value) /*s*/, 0 /*us*/);

        /// Unwrap each text argument, handling Nullable and Const uniformly (as `aiEmbed` does). A Nullable
        /// column can arrive as `ColumnNullable` or `ColumnConst(ColumnNullable)`. `convertToFullColumnIfConst`
        /// reduces the latter to the former so a single null-map path covers both. `holder` keeps it alive.
        struct Operand
        {
            ColumnPtr holder;
            const ColumnNullable * nullable = nullptr;
            const IColumn * data = nullptr;
        };
        auto prepare = [&](size_t arg_index) -> Operand
        {
            Operand op;
            if (arguments[arg_index].type->isNullable())
            {
                op.holder = arguments[arg_index].column->convertToFullColumnIfConst();
                op.nullable = typeid_cast<const ColumnNullable *>(op.holder.get());
                op.data = &op.nullable->getNestedColumn();
            }
            else
            {
                op.data = arguments[arg_index].column.get();
            }
            return op;
        };
        Operand op1 = prepare(0);
        Operand op2 = prepare(1);

        /// A row's operand contributes an embedding only when it is non-null and non-empty.
        auto liveValue = [](const Operand & op, size_t row, std::string_view & out) -> bool
        {
            if (op.nullable && op.nullable->getNullMapData()[row])
                return false;
            out = op.data->getDataAt(row);
            return !out.empty();
        };

        /// Deduplicate operands across both columns so each distinct text is embedded once per block
        /// (e.g. a constant query text, or values repeated across a self-join). `left`/`right` map each
        /// row's operands to an index into `inputs`, or `no_input` when the operand does not embed.
        std::unordered_map<std::string_view, size_t> input_index; // STYLE_CHECK_ALLOW_STD_CONTAINERS
        VectorWithMemoryTracking<std::string_view> inputs;
        constexpr size_t no_input = std::numeric_limits<size_t>::max();
        VectorWithMemoryTracking<size_t> left(input_rows_count, no_input);
        VectorWithMemoryTracking<size_t> right(input_rows_count, no_input);

        auto intern = [&](std::string_view text) -> size_t
        {
            auto [it, inserted] = input_index.try_emplace(text, inputs.size());
            if (inserted)
                inputs.push_back(text);
            return it->second;
        };

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            std::string_view text;
            if (liveValue(op1, i, text))
                left[i] = intern(text);
            if (liveValue(op2, i, text))
                right[i] = intern(text);
        }

        auto embedding_result = FunctionBaseAI::embedTexts(
            *provider, model, dimensions, inputs, max_batch_size, max_retries, retry_delay_ms, throw_on_error, quota, timeouts);

        ProfileEvents::increment(ProfileEvents::AIAPICalls, embedding_result.api_calls);
        ProfileEvents::increment(ProfileEvents::AIInputTokens, embedding_result.input_tokens);
        ProfileEvents::increment(ProfileEvents::AIRowsProcessed, embedding_result.texts_embedded);
        ProfileEvents::increment(ProfileEvents::AIRowsSkipped, embedding_result.texts_skipped);

        const auto & embeddings = embedding_result.embeddings;

        auto score_col = ColumnFloat32::create();
        auto null_map_col = ColumnUInt8::create(input_rows_count, static_cast<UInt8>(0));
        auto & scores = score_col->getData();
        auto & null_map = null_map_col->getData();
        scores.resize(input_rows_count);

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            size_t a = left[i];
            size_t b = right[i];
            /// NULL when either operand had no text, its embedding was skipped (quota/error), or the two
            /// vectors are not comparable (same model should always yield equal sizes, guarded regardless).
            if (a == no_input || b == no_input || embeddings[a].empty() || embeddings[b].empty()
                || embeddings[a].size() != embeddings[b].size())
            {
                scores[i] = 0;
                null_map[i] = 1;
                continue;
            }

            scores[i] = cosineSimilarity(embeddings[a], embeddings[b], null_map[i]);
        }

        return ColumnNullable::create(std::move(score_col), std::move(null_map_col));
    }

private:
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

Both texts are embedded (reusing `aiEmbed`'s batching and server-side provider configuration) and the
function returns the cosine similarity of the two vectors, in the range `[-1, 1]`: `1` means the texts
are semantically identical, `0` means unrelated, and negative values mean opposite. This is the
complement of `cosineDistance` over the same embeddings (`aiSimilarity = 1 - cosineDistance`).

Distinct operand values are embedded only once per block, so a constant query text or values repeated
across a self-join are not re-embedded per row. Batching, credentials, and the `dimensions` parameter
match `aiEmbed`, including the `ai_function_embedding_default_credentials` default-credentials setting.

Like `aiEmbed`, `model` is a required positional argument (a constant `String`), not read from the
named collection or the parameter map.
)",
        .syntax = "aiSimilarity(text1, text2, model[, params])",
        .arguments
        = {{"text1", "First text.", {"String"}},
           {"text2", "Second text.", {"String"}},
           {"model", "Embedding model name.", {"const String"}},
           {"params", "Optional constant `Map(String, String)` of parameters. Function-specific key: `dimensions` (target dimensionality of the embeddings; `0` or omitted means the model's native size). The common parameter `credentials` also applies (see [AI Functions](/sql-reference/functions/ai-functions)).", {"Map(String, String)"}}},
        .returned_value = {"The cosine similarity in `[-1, 1]`, or NULL if either text is NULL or empty, an embedding request failed and `ai_function_throw_on_error` is disabled, or a quota was exceeded with `ai_function_throw_on_quota_exceeded` disabled.", {"Nullable(Float32)"}},
        .examples
        = {{"Compare two strings (`credentials` can be omitted if the `ai_function_embedding_default_credentials` setting is set)", "SELECT aiSimilarity('cat', 'kitten', 'text-embedding-3-small', map('credentials', 'ai_embedding_credentials'))", ""},
           {"Rank reviews by similarity to a query", "SELECT review FROM product_reviews ORDER BY aiSimilarity(review, 'It works well under rain', 'text-embedding-3-small') DESC LIMIT 100", ""},
           {"Semantic dedup over a self-join", "SELECT a.id, b.id FROM docs a, docs b WHERE a.id < b.id AND aiSimilarity(a.title, b.title, 'text-embedding-3-small') > 0.9", ""}},
        .introduced_in = {26, 7},
        .category = FunctionDocumentation::Category::AI});

    factory.registerAlias("AISimilarity", "aiSimilarity");
}

}
