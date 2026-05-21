#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionBaseAI.h>
#include <Functions/AI/IAIProvider.h>
#include <Functions/AI/AIQuotaTracker.h>

#include <Common/ProfileEvents.h>
#include <Common/Exception.h>
#include <Common/RemoteHostFilter.h>

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

#include <thread> /// thread::sleep for retry backoff

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
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int RECEIVED_ERROR_FROM_REMOTE_IO_SERVER;
    extern const int SUPPORT_IS_DISABLED;
}

namespace
{

class FunctionAiEmbed final : public IFunction
{
public:
    static constexpr auto name = "aiEmbed";

    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionAiEmbed>(context); }

    explicit FunctionAiEmbed(ContextPtr context_) : context(context_)
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
            {"collection", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), &isColumnConst, "const String"},
            {"text", static_cast<FunctionArgumentDescriptor::TypeValidator>(&FunctionBaseAI::isStringOrNullableString), nullptr, "String or Nullable(String)"},
        };
        FunctionArgumentDescriptors optional_args{
            {"dimensions", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isNativeUInt), &isColumnConst, "const UInt"},
        };
        validateFunctionArguments(*this, arguments, mandatory_args, optional_args);

        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat32>());
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        auto nc = FunctionBaseAI::resolveAINamedCollection(getContext(), arguments[0].column);

        if (input_rows_count == 0)
            return result_type->createColumn();

        UInt64 dimensions = 0;
        if (arguments.size() > 2)
        {
            const auto * dim_const = typeid_cast<const ColumnConst *>(arguments[2].column.get());
            chassert(dim_const, "dimensions must be a constant UInt (validated by getReturnTypeImpl)");
            dimensions = dim_const->getUInt(0);

            /// Providers serialize `dimensions` as Int64 (Poco JSON does not support UInt64).
            /// Reject values that would silently become negative after the cast.
            if (dimensions > static_cast<UInt64>(std::numeric_limits<Int64>::max()))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "aiEmbed: 'dimensions' exceeds maximum ({})",
                    std::numeric_limits<Int64>::max());
        }

        const auto & settings = getContext()->getSettingsRef();
        UInt64 timeout_sec = settings[Setting::ai_function_request_timeout_sec].value;
        UInt64 max_retries = settings[Setting::ai_function_max_retries].value;
        UInt64 retry_delay_ms = settings[Setting::ai_function_retry_initial_delay_ms].value;
        bool throw_on_error = settings[Setting::ai_function_throw_on_error].value;
        size_t max_batch_size = static_cast<size_t>(settings[Setting::ai_function_embedding_max_batch_size].value);

        AIQuotaTracker quota(
            settings[Setting::ai_function_max_input_tokens_per_query].value,
            settings[Setting::ai_function_max_output_tokens_per_query].value,
            settings[Setting::ai_function_max_api_calls_per_query].value,
            settings[Setting::ai_function_throw_on_quota_exceeded].value);

        auto provider = createAIProvider(nc.provider, nc.endpoint, nc.api_key, nc.api_version);

        auto timeouts = ConnectionTimeouts::getHTTPTimeouts(settings, getContext()->getServerSettings());
        timeouts.receive_timeout = Poco::Timespan(static_cast<int64_t>(timeout_sec) /*s*/, 0 /*us*/);

        /// A Nullable text column can arrive as `ColumnNullable` or as `ColumnConst(ColumnNullable)` (e.g. `NULL::Nullable(String)`).
        /// `convertToFullColumnIfConst` unwraps the latter into the former, so a single null-map path handles both.
        ColumnPtr text_column;
        const ColumnNullable * text_nullable = nullptr;
        if (arguments[text_arg_index].type->isNullable())
        {
            text_column = arguments[text_arg_index].column->convertToFullColumnIfConst();
            text_nullable = typeid_cast<const ColumnNullable *>(text_column.get());
        }
        const IColumn & text_data_column = text_nullable
            ? text_nullable->getNestedColumn()
            : *arguments[text_arg_index].column;

        /// Collect the indices of rows that actually need an HTTP call: non-null and non-empty.
        /// Both null and empty-string rows map to `[]` in the output
        std::vector<size_t> live_rows;
        live_rows.reserve(input_rows_count);
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            if (text_nullable && text_nullable->getNullMapData()[i])
                continue;
            if (text_data_column.getDataAt(i).empty())
                continue;
            live_rows.push_back(i);
        }

        auto data_col = ColumnVector<Float32>::create(); /// float32 is standard embedding API output
        auto offsets_col = ColumnArray::ColumnOffsets::create();
        auto & data_vec = data_col->getData();
        auto & offsets_vec = offsets_col->getData();
        offsets_vec.reserve(input_rows_count);

        /// If dimensions is set we can reserve, otherwise we don't know what the dimension will be
        if (dimensions > 0)
            data_vec.reserve(live_rows.size() * dimensions);

        UInt64 total_api_calls = 0;
        UInt64 total_input_tokens = 0;
        UInt64 rows_processed = 0; /// rows that received an AI result
        UInt64 rows_skipped = 0; /// rows that received a default value due to quota or error
        UInt64 current_offset = 0;

        size_t cursor = 0;

        for (size_t batch_start = 0; batch_start < live_rows.size(); batch_start += max_batch_size)
        {
            if (quota.checkQuotas())
            {
                rows_skipped += live_rows.size() - batch_start;
                break;
            }

            size_t batch_end = std::min(batch_start + max_batch_size, live_rows.size());

            AIEmbeddingRequest ai_embedding_request;
            ai_embedding_request.model = nc.model;
            ai_embedding_request.dimensions = dimensions;
            ai_embedding_request.inputs.reserve(batch_end - batch_start);

            for (size_t k = batch_start; k < batch_end; ++k)
                ai_embedding_request.inputs.emplace_back(text_data_column.getDataAt(live_rows[k]));

            AIEmbeddingResponse ai_embedding_response;
            bool batch_ok = false;
            for (UInt64 attempt = 0; attempt <= max_retries; ++attempt)
            {
                try
                {
                    ai_embedding_response = provider->embed(ai_embedding_request, timeouts);
                    ++total_api_calls;
                    total_input_tokens += ai_embedding_response.input_tokens;
                    quota.recordResponse(ai_embedding_response.input_tokens, 0);
                    batch_ok = true;
                    break;
                }
                catch (const Exception & e)
                {
                    if (attempt < max_retries && e.code() == ErrorCodes::RECEIVED_ERROR_FROM_REMOTE_IO_SERVER)
                    {
                        std::this_thread::sleep_for(std::chrono::milliseconds(FunctionBaseAI::computeRetryBackoffMs(retry_delay_ms, attempt)));
                        continue;
                    }

                    if (!throw_on_error) /// just skip to next batch, this batch's rows will be filled with empty arrays
                        break;

                    throw;
                }
                catch (...) /// Handle non-DB exceptions (e.g. Poco network/JSON errors) for throw_on_error semantics
                {
                    if (!throw_on_error) /// just skip to next batch, this batch's rows will be filled with empty arrays
                        break;

                    throw;
                }
            }

            if (!batch_ok) /// failed batch's rows are filled in by the next batch (or the final tail fill)
            {
                rows_skipped += batch_end - batch_start;
                continue;
            }

            chassert(ai_embedding_response.embeddings.size() == ai_embedding_request.inputs.size(),
                "Number of inputs does not match number of output embeddings");

            for (size_t k = 0; k < ai_embedding_response.embeddings.size(); ++k)
            {
                /// fill pre-filtered (NULL/empty) input rows that sit between consecutive live rows
                size_t last_row_in_batch = live_rows[batch_start + k];
                for (; cursor < last_row_in_batch; ++cursor)
                    offsets_vec.push_back(current_offset);

                const auto & v = ai_embedding_response.embeddings[k];
                data_vec.insert(data_vec.end(), v.begin(), v.end());
                current_offset += v.size();
                offsets_vec.push_back(current_offset);
                ++cursor;
                ++rows_processed;
            }
        }

        /// fill final empties (pre-filtered tail rows, plus any live rows already counted into rows_skipped)
        for (; cursor < input_rows_count; ++cursor)
            offsets_vec.push_back(current_offset);

        ProfileEvents::increment(ProfileEvents::AIAPICalls, total_api_calls);
        ProfileEvents::increment(ProfileEvents::AIInputTokens, total_input_tokens);
        ProfileEvents::increment(ProfileEvents::AIRowsProcessed, rows_processed);
        ProfileEvents::increment(ProfileEvents::AIRowsSkipped, rows_skipped);

        return ColumnArray::create(std::move(data_col), std::move(offsets_col));
    }

private:
    static constexpr size_t text_arg_index = 1;

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
[`ai_function_embedding_max_batch_size`](/operations/settings/settings#ai_function_embedding_max_batch_size)
entries per HTTP request to reduce per-call overhead.

The first argument is a named collection that specifies the provider, model, endpoint, and API key.
The optional `dimensions` argument, when supported by the model (e.g. OpenAI's `text-embedding-3-*`),
requests a vector of the given size; otherwise the model's native size is returned.
)",
        .syntax = "aiEmbed(collection, text[, dimensions])",
        .arguments
        = {{"collection", "Name of a named collection containing provider credentials and configuration.", {"String"}},
           {"text", "Text to embed.", {"String"}},
           {"dimensions", "Optional target dimensionality for the output vector. `0` or omitted means the model's native size.", {"UInt64"}}},
        .returned_value = {"The embedding vector, or an empty array if the input is NULL or empty, the request failed and `ai_function_throw_on_error` is disabled, or a quota was exceeded with `ai_function_throw_on_quota_exceeded` disabled.", {"Array(Float32)"}},
        .examples
        = {{"Embed a single string", "SELECT aiEmbed('ai_credentials', 'Hello world')", ""},
           {"With explicit dimensions", "SELECT aiEmbed('ai_credentials', 'Hello world', 256)", ""},
           {"Embed a column of texts", "SELECT aiEmbed('ai_credentials', title, 256) FROM articles LIMIT 10", ""}},
        .introduced_in = {26, 6},
        .category = FunctionDocumentation::Category::AI});
}

}
