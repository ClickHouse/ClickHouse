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

#include <thread>
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

    explicit FunctionAiEmbed(ContextPtr context_) : context_weak(context_)
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

    /// Handle Nullable cols explicitly, since setting this to true may call functions with arbitrary input values
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

        return FunctionBaseAI::wrapReturnTypeForNullablePrompt(arguments, text_arg_index, std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat32>()));
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        if (input_rows_count == 0)
            return result_type->createColumn();

        auto nc = FunctionBaseAI::resolveAINamedCollection(getContext(), arguments[0].column);
        getContext()->getRemoteHostFilter().checkURL(Poco::URI(nc.endpoint));

        UInt64 dimensions = 0;
        if (arguments.size() > 2)
        {
            const auto * dim_const = typeid_cast<const ColumnConst *>(arguments[2].column.get());
            chassert(dim_const, "dimensions must be a constant UInt (validated by getReturnTypeImpl)");
            dimensions = dim_const->getUInt(0);
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

        /// Deduplicate identical texts within the batch: each unique text is sent once and the result is reused for every row that had it.
        std::unordered_map<String, std::vector<size_t>> dedup_map;
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            if (text_nullable && text_nullable->getNullMapData()[i])
                continue;
            String text(text_data_column.getDataAt(i));
            if (!text.empty())
                dedup_map[std::move(text)].push_back(i);
        }

        std::vector<String> unique_texts;
        unique_texts.reserve(dedup_map.size());
        for (const auto & [text, _] : dedup_map)
            unique_texts.push_back(text);

        std::unordered_map<String, std::vector<Float32>> results;

        UInt64 total_api_calls = 0;
        UInt64 total_input_tokens = 0;

        for (size_t batch_start = 0; batch_start < unique_texts.size(); batch_start += max_batch_size)
        {
            if (quota.checkQuotas())
                break;

            size_t batch_end = std::min(batch_start + max_batch_size, unique_texts.size());

            AIEmbeddingRequest ai_embedding_request;
            ai_embedding_request.model = nc.model;
            ai_embedding_request.dimensions = dimensions;
            ai_embedding_request.inputs.assign(unique_texts.begin() + batch_start, unique_texts.begin() + batch_end);

            for (UInt64 attempt = 0; attempt <= max_retries; ++attempt)
            {
                try
                {
                    auto ai_embedding_response = provider->embed(ai_embedding_request, timeouts);
                    ++total_api_calls;
                    total_input_tokens += ai_embedding_response.input_tokens;
                    quota.recordResponse(ai_embedding_response.input_tokens, 0);

                    for (size_t i = 0; i < ai_embedding_request.inputs.size(); ++i)
                    {
                        if (i < ai_embedding_response.embeddings.size())
                            results[ai_embedding_request.inputs[i]] = std::move(ai_embedding_response.embeddings[i]);
                    }
                    break;
                }
                catch (const Exception & e)
                {
                    if (attempt < max_retries && e.code() == ErrorCodes::RECEIVED_ERROR_FROM_REMOTE_IO_SERVER)
                    {
                        std::this_thread::sleep_for(std::chrono::milliseconds(retry_delay_ms * (1ULL << std::min(attempt, UInt64(63)))));
                        continue;
                    }

                    if (!throw_on_error) /// just skip to next batch, this batch's results will be filled with default vals
                        break;

                    throw;
                }
                catch (...) /// Handle non-DB exceptions (e.g. Poco network/JSON errors) for throw_on_error semantics
                {
                    if (!throw_on_error) /// just skip to next batch, this batch's results will be filled with default vals
                        break;

                    throw;
                }
            }
        }

        auto data_col = ColumnVector<Float32>::create();
        auto offsets_col = ColumnArray::ColumnOffsets::create();
        auto & data_vec = data_col->getData();
        auto & offsets_vec = offsets_col->getData();
        offsets_vec.reserve(input_rows_count);

        UInt64 rows_processed = 0;
        UInt64 rows_skipped = 0;
        UInt64 current_offset = 0;

        std::vector<const std::vector<Float32> *> row_to_embedding(input_rows_count, nullptr);
        for (const auto & [text, rows] : dedup_map)
        {
            auto it = results.find(text);
            if (it == results.end() || it->second.empty())
                continue;
            for (size_t row : rows)
                row_to_embedding[row] = &it->second;
        }

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            if (row_to_embedding[i])
            {
                const auto & vec = *row_to_embedding[i];
                data_vec.insert(data_vec.end(), vec.begin(), vec.end());
                current_offset += vec.size();
                ++rows_processed;
            }
            else
            {
                ++rows_skipped;
            }
            offsets_vec.push_back(current_offset);
        }

        ProfileEvents::increment(ProfileEvents::AIAPICalls, total_api_calls);
        ProfileEvents::increment(ProfileEvents::AIInputTokens, total_input_tokens);
        ProfileEvents::increment(ProfileEvents::AIRowsProcessed, rows_processed);
        ProfileEvents::increment(ProfileEvents::AIRowsSkipped, rows_skipped);

        ColumnPtr array_col = ColumnArray::create(std::move(data_col), std::move(offsets_col));
        if (result_type->isNullable())
        {
            auto null_map_col = ColumnUInt8::create(input_rows_count, static_cast<UInt8>(0));
            if (text_nullable)
            {
                auto & null_map_data = null_map_col->getData();
                const auto & src_null_map = text_nullable->getNullMapData();
                for (size_t i = 0; i < input_rows_count; ++i)
                    null_map_data[i] = src_null_map[i];
            }
            return ColumnNullable::create(std::move(array_col), std::move(null_map_col));
        }
        return array_col;
    }

private:
    static constexpr size_t text_arg_index = 1;

    ContextWeakPtr context_weak;
    ContextPtr getContext() const { return context_weak.lock(); }
};

}

REGISTER_FUNCTION(AiEmbed)
{
    factory.registerFunction<FunctionAiEmbed>(FunctionDocumentation{
        .description = R"(
Generates an embedding vector for the given text using the configured AI provider.

The function sends the text to the configured embedding endpoint and returns the resulting vector as `Array(Float32)`.
Identical texts within a query are deduplicated and sent to the provider once. Unique texts are grouped into
batches of up to [`ai_function_embedding_max_batch_size`](/operations/settings/settings#ai_function_embedding_max_batch_size)
entries per HTTP request.

The first argument is a named collection that specifies the provider, model, endpoint, and API key.
The optional `dimensions` argument, when supported by the model (e.g. OpenAI's `text-embedding-3-*`),
requests a vector of the given size; otherwise the model's native size is returned.
)",
        .syntax = "aiEmbed(collection, text[, dimensions])",
        .arguments
        = {{"collection", "Name of a named collection containing provider credentials and configuration.", {"String"}},
           {"text", "Text to embed.", {"String"}},
           {"dimensions", "Optional target dimensionality for the output vector. `0` or omitted means the model's native size.", {"UInt64"}}},
        .returned_value = {"The embedding vector, or an empty array if the input is empty, the request failed and `ai_function_throw_on_error` is disabled, or a quota was exceeded with `ai_function_throw_on_quota_exceeded` disabled.", {"Array(Float32)"}},
        .examples
        = {{"Embed a single string", "SELECT aiEmbed('ai_credentials', 'Hello world')", ""},
           {"With explicit dimensions", "SELECT aiEmbed('ai_credentials', 'Hello world', 256)", ""},
           {"Embed a column of texts", "SELECT aiEmbed('ai_credentials', title, 256) FROM articles LIMIT 10", ""}},
        .introduced_in = {26, 4},
        .category = FunctionDocumentation::Category::AI});
}

}
