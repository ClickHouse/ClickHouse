#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionBaseAI.h>
#include <Functions/AI/IAIProvider.h>
#include <Functions/AI/AIQuotaTracker.h>

#include <Common/ProfileEvents.h>
#include <Common/Exception.h>
#include <Common/RemoteHostFilter.h>

#include <base/scope_guard.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>

#include <IO/ConnectionTimeouts.h>
#include <Core/Settings.h>
#include <Core/ServerSettings.h>
#include <Interpreters/Context.h>

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

class FunctionAiEmbed final : public IFunction
{
public:
    static constexpr auto name = "aiEmbed";

    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionAiEmbed>(context); }

    explicit FunctionAiEmbed(ContextPtr context_) : context(context_) {}

    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    /// func has side effects, f.e. each call updates quota, makes potentially expensive outside call, etc.
    bool isStateful() const override { return true; }

    /// Unlike the other AI funcs, it's ok and actually preferable for
    /// optimizer to fold calls with same args together
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
            {"text", static_cast<FunctionArgumentDescriptor::TypeValidator>(&FunctionBaseAI::isStringOrNullableString), nullptr, "String"},
            {"model", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), &isColumnConst, "const String"},
        };
        FunctionArgumentDescriptors optional_args{
            {"params", static_cast<FunctionArgumentDescriptor::TypeValidator>(&FunctionBaseAI::isStringToStringMap), &isColumnConst, "const Map(String, String)"},
        };
        validateFunctionArguments(*this, arguments, mandatory_args, optional_args);

        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat32>());
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

        UInt64 timeout_sec = settings[Setting::ai_function_request_timeout_sec].value;
        UInt64 max_retries = settings[Setting::ai_function_max_retries].value;
        UInt64 retry_delay_ms = settings[Setting::ai_function_retry_initial_delay_ms].value;
        bool throw_on_error = settings[Setting::ai_function_throw_on_error].value;
        size_t max_batch_size = static_cast<size_t>(settings[Setting::ai_function_embedding_max_batch_size].value);

        /// Shared across every AI function call in the query
        auto quota_tracker = getContext()->getAIQuotaTracker();

        auto timeouts = ConnectionTimeouts::getHTTPTimeouts(settings, getContext()->getServerSettings());
        timeouts.receive_timeout = Poco::Timespan(static_cast<int64_t>(timeout_sec) /*s*/, 0 /*us*/);

        /// `isNullAt` and `getDataAt` are virtual on `IColumn`, so a single path covers `ColumnString`,
        /// `ColumnConst(ColumnString)`, `ColumnNullable` and `ColumnConst(ColumnNullable)` (e.g.
        /// `NULL::Nullable(String)`). A constant is read at index 0 rather than materialized.
        const IColumn & text_column = *arguments[text_arg_index].column;

        /// A row needs an HTTP call only when non-null and non-empty; both null and empty-string rows map
        /// to `[]` in the output. The `isNullAt` check also guards the only case where
        /// `ColumnNullable::getDataAt` throws (a NULL value).
        auto get_value = [](const IColumn & column, size_t row, std::string_view & out) -> bool
        {
            if (column.isNullAt(row))
                return false;
            out = column.getDataAt(row);
            return !out.empty();
        };

        VectorWithMemoryTracking<size_t> live_rows;
        VectorWithMemoryTracking<std::string_view> inputs;
        live_rows.reserve(input_rows_count);
        inputs.reserve(input_rows_count);
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            std::string_view value;
            if (!get_value(text_column, i, value))
                continue;
            live_rows.push_back(i);
            inputs.push_back(value);
        }

        /// One text per row here, so the text counts are the row counts. Increment upon destruction,
        /// to avoid underreporting on error.
        FunctionBaseAI::EmbeddingResult embedding_result;
        SCOPE_EXIT({
            ProfileEvents::increment(ProfileEvents::AIRowsProcessed, embedding_result.texts_embedded);
            ProfileEvents::increment(ProfileEvents::AIRowsSkipped, embedding_result.texts_skipped);
        });

        FunctionBaseAI::embedTexts(
            *provider, model, dimensions, getName(), inputs, max_batch_size, max_retries, retry_delay_ms, throw_on_error, *quota_tracker,
            timeouts, embedding_result);

        auto data_col = ColumnVector<Float32>::create(); /// float32 is standard embedding API output
        auto offsets_col = ColumnArray::ColumnOffsets::create();
        auto & data_vec = data_col->getData();
        auto & offsets_vec = offsets_col->getData();
        offsets_vec.reserve(input_rows_count);

        /// If dimensions is set we can reserve, otherwise we don't know what the dimension will be
        if (dimensions > 0)
            data_vec.reserve(live_rows.size() * dimensions);

        UInt64 current_offset = 0;
        size_t cursor = 0;
        for (size_t k = 0; k < live_rows.size(); ++k)
        {
            /// fill pre-filtered (NULL/empty) input rows that sit between consecutive live rows
            for (; cursor < live_rows[k]; ++cursor)
                offsets_vec.push_back(current_offset);

            /// a skipped input (quota/error) has an empty embedding, so its row also becomes `[]`
            const auto & v = embedding_result.embeddings[k];
            data_vec.insert(data_vec.end(), v.begin(), v.end());
            current_offset += v.size();
            offsets_vec.push_back(current_offset);
            ++cursor;
        }

        /// fill final empties (pre-filtered tail rows and any trailing skipped inputs)
        for (; cursor < input_rows_count; ++cursor)
            offsets_vec.push_back(current_offset);

        return ColumnArray::create(std::move(data_col), std::move(offsets_col));
    }

private:
    static constexpr size_t text_arg_index = 0;
    static constexpr size_t model_arg_index = 1;

    ContextPtr context;
    ContextPtr getContext() const { return context; }
};

}

REGISTER_FUNCTION(AiEmbed)
{
    factory.registerFunction<FunctionAiEmbed>(FunctionDocumentation{
        .description = R"(
Generates an embedding vector for the given text using the configured AI provider.

The function sends the text to the configured embedding endpoint and returns the resulting vector as `Array(Float32)`.
Within a single block of rows, inputs are grouped into batches of up to
[`ai_function_embedding_max_batch_size`](/reference/settings/session-settings/ai-function#ai_function_embedding_max_batch_size)
entries per HTTP request to reduce per-call overhead.

Credentials (a named collection specifying the provider, endpoint, and optionally an API key)
are taken from the `credentials` key of the parameter map, or from the
`ai_function_embedding_default_credentials` setting when the map omits it. Note that `aiEmbed` uses a
separate default-credentials setting from the text functions, since an embeddings endpoint differs
from a chat one.

The `model` is a required positional argument (a constant `String`). Unlike the text functions,
`aiEmbed` does not read `model` from the named collection or the parameter map. A named collection
that defines `model` is rejected.

The optional `dimensions` parameter, when supported by the model (e.g. OpenAI's `text-embedding-3-*`),
requests a vector of the given size; otherwise the model's native size is returned.
)",
        .syntax = "aiEmbed(text, model[, params])",
        .arguments
        = {{"text", "Text to embed.", {"String"}},
           {"model", "Embedding model name.", {"const String"}},
           {"params", "Optional constant `Map(String, String)` of parameters. Function-specific key: `dimensions` (target dimensionality of the output vector; `0` or omitted means the model's native size). The common parameter `credentials` also applies (see [AI Functions](/reference/functions/regular-functions/ai-functions)).", {"Map(String, String)"}}},
        .returned_value = {"The embedding vector, or an empty array if the input is NULL or empty, the request failed and `ai_function_throw_on_error` is disabled, or a quota was exceeded with `ai_function_throw_on_quota_exceeded` disabled.", {"Array(Float32)"}},
        .examples
        = {{"Embed a single string (`credentials` can be omitted if the `ai_function_embedding_default_credentials` setting is set)", "SELECT aiEmbed('Hello world', 'text-embedding-3-small', map('credentials', 'ai_embedding_credentials'))", ""},
           {"With explicit dimensions", "SELECT aiEmbed('Hello world', 'text-embedding-3-small', map('credentials', 'ai_embedding_credentials', 'dimensions', '256'))", ""},
           {"Embed a column of texts", "CREATE TABLE articles (title String) ENGINE = Memory;\nINSERT INTO articles VALUES ('ClickHouse is a fast analytical database.');\nSELECT aiEmbed(title, 'text-embedding-3-small', map('credentials', 'ai_embedding_credentials', 'dimensions', '256')) FROM articles LIMIT 10", ""}},
        .introduced_in = {26, 6},
        .category = FunctionDocumentation::Category::AI});

    factory.registerAlias("AIEmbed", "aiEmbed");
}

}
