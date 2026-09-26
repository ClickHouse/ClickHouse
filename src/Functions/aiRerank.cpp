#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionBaseAI.h>
#include <Functions/AI/IAIProvider.h>
#include <Functions/AI/AIQuotaTracker.h>

#include <Common/ProfileEvents.h>
#include <Common/Exception.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Common/assert_cast.h>
#include <base/scope_guard.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>

#include <IO/ConnectionTimeouts.h>
#include <Core/Settings.h>
#include <Core/ServerSettings.h>
#include <Interpreters/Context.h>

#include <chrono>
#include <string_view>
#include <thread>

namespace ProfileEvents
{
    extern const Event AIAPICalls;
    extern const Event AIInputTokens;
    extern const Event AIOutputTokens;
    extern const Event AISearchUnits;
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
    extern const SettingsString ai_function_rerank_default_credentials;
    extern const SettingsBool allow_experimental_ai_rerank_function;
}

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int SUPPORT_IS_DISABLED;
}

namespace
{

bool isArrayOfStrings(const IDataType & type)
{
    const auto * array_type = typeid_cast<const DataTypeArray *>(&type);
    return (array_type && isString(array_type->getNestedType()));
}

class FunctionAiRerank final : public IFunction
{
public:
    static constexpr auto name = "aiRerank";

    static FunctionPtr create(ContextPtr context)
    {
        if (!context->getSettingsRef()[Setting::allow_experimental_ai_rerank_function])
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                "Function '{}' is experimental. Set `allow_experimental_ai_rerank_function` setting to enable it", name);

        return std::make_shared<FunctionAiRerank>(context);
    }

    explicit FunctionAiRerank(ContextPtr context_) : context(context_) {}

    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    /// func has side effects, f.e. each call updates quota, makes potentially expensive outside call, etc.
    bool isStateful() const override { return true; }

    /// Rerankers are not guaranteed to return the same scores or order for the same input across calls
    /// (model updates behind the same name, provider-side nondeterminism), so results must not be reused
    /// across queries. Within a query, folding calls with identical args together is preferable: it keeps
    /// the results consistent and avoids paying for duplicate API calls.
    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override { return true; }

    bool isSuitableForConstantFolding() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }

    /// Handle Nullable cols explicitly, since setting this to true may call func with arbitrary input values
    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return false; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors mandatory_args{
            {"query", static_cast<FunctionArgumentDescriptor::TypeValidator>(&FunctionBaseAI::isStringOrNullableString), nullptr, "String"},
            {"documents", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isArrayOfStrings), nullptr, "Array(String)"},
        };
        FunctionArgumentDescriptors optional_args{
            {"params", static_cast<FunctionArgumentDescriptor::TypeValidator>(&FunctionBaseAI::isStringToStringMap), &isColumnConst, "const Map(String, String)"},
        };
        validateFunctionArguments(*this, arguments, mandatory_args, optional_args);

        /// Always non-Nullable: `Nullable(Array(...))` is not a valid ClickHouse type. A NULL/empty
        /// query, an empty `documents` array, or a skipped row (quota/error) all map to `[]`.
        DataTypePtr tuple = std::make_shared<DataTypeTuple>(
            DataTypes{std::make_shared<DataTypeUInt32>(), std::make_shared<DataTypeFloat32>()},
            Names{"index", "relevance_score"});
        return std::make_shared<DataTypeArray>(tuple);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const auto & settings = getContext()->getSettingsRef();
        auto params = FunctionBaseAI::resolveAIParams(
            getContext(), arguments, paramSpecs(), settings[Setting::ai_function_rerank_default_credentials]);

        UInt64 top_n = params.getUInt("top_n");
        String model = params.getString("model");

        auto provider = createAIProvider(
            params.collection.provider, params.collection.endpoint, params.collection.api_key, params.collection.api_version);
        if (!provider->supportsRerank())
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                "AI provider '{}' does not support reranking", params.collection.provider);

        if (input_rows_count == 0)
            return result_type->createColumn();

        UInt64 timeout_sec = settings[Setting::ai_function_request_timeout_sec].value;
        UInt64 max_retries = settings[Setting::ai_function_max_retries].value;
        UInt64 retry_delay_ms = settings[Setting::ai_function_retry_initial_delay_ms].value;
        bool throw_on_error = settings[Setting::ai_function_throw_on_error].value;

        /// Shared across every AI function call in the query
        auto quota_tracker = getContext()->getAIQuotaTracker();

        auto timeouts = ConnectionTimeouts::getHTTPTimeouts(settings, getContext()->getServerSettings());
        timeouts.receive_timeout = Poco::Timespan(static_cast<int64_t>(timeout_sec) /*s*/, 0 /*us*/);

        /// `isNullAt` and `getDataAt` are virtual on `IColumn`, so a single path covers `ColumnString`,
        /// `ColumnConst(ColumnString)`, `ColumnNullable` and `ColumnConst(ColumnNullable)` (e.g.
        /// `NULL::Nullable(String)`). A constant query is read at index 0 rather than materialized.
        const IColumn & query_column = *arguments[query_arg_index].column;

        /// A constant array literal arrives as `ColumnConst` holding a single-row `ColumnArray`: read its
        /// row 0 for every input row rather than materializing a copy per row.
        const IColumn & documents_arg = *arguments[documents_arg_index].column;
        const auto * documents_const = checkAndGetColumn<ColumnConst>(&documents_arg);
        const auto & documents_array
            = assert_cast<const ColumnArray &>(documents_const ? documents_const->getDataColumn() : documents_arg);
        const IColumn & documents_data = documents_array.getData();
        const auto & documents_offsets = documents_array.getOffsets();

        auto index_col = ColumnUInt32::create();
        auto score_col = ColumnFloat32::create();
        auto offsets_col = ColumnArray::ColumnOffsets::create();
        auto & offsets = offsets_col->getData();
        offsets.reserve(input_rows_count);

        UInt64 total_api_calls = 0;
        UInt64 total_input_tokens = 0;
        UInt64 total_output_tokens = 0;
        UInt64 total_search_units = 0;
        UInt64 rows_processed = 0;
        UInt64 rows_skipped = 0;

        /// Increment ProfileEvents counters upon destruction, to avoid underreporting on error
        SCOPE_EXIT({
            ProfileEvents::increment(ProfileEvents::AIAPICalls, total_api_calls);
            ProfileEvents::increment(ProfileEvents::AIInputTokens, total_input_tokens);
            ProfileEvents::increment(ProfileEvents::AIOutputTokens, total_output_tokens);
            ProfileEvents::increment(ProfileEvents::AISearchUnits, total_search_units);
            ProfileEvents::increment(ProfileEvents::AIRowsProcessed, rows_processed);
            ProfileEvents::increment(ProfileEvents::AIRowsSkipped, rows_skipped);
        });

        UInt64 current_offset = 0;
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            size_t documents_row = documents_const ? 0 : i;
            size_t docs_begin = (documents_row == 0) ? 0 : documents_offsets[documents_row - 1];
            size_t docs_end = documents_offsets[documents_row];

            /// A NULL/empty query or an empty document list has nothing to rank: skip the HTTP call
            /// entirely and emit `[]`.
            if (query_column.isNullAt(i) || docs_end == docs_begin)
            {
                offsets.push_back(current_offset);
                continue;
            }

            std::string_view query = query_column.getDataAt(i);
            if (query.empty())
            {
                offsets.push_back(current_offset);
                continue;
            }

            if (quota_tracker->checkQuotas())
            {
                offsets.push_back(current_offset);
                ++rows_skipped;
                continue;
            }

            AIRerankRequest ai_rerank_request;
            ai_rerank_request.query = String(query);
            ai_rerank_request.documents.reserve(docs_end - docs_begin);
            /// Documents are never filtered (even empty strings are sent verbatim): the provider's
            /// `index` in the response refers to the position within this exact list.
            for (size_t j = docs_begin; j < docs_end; ++j)
                ai_rerank_request.documents.push_back(documents_data.getDataAt(j));
            ai_rerank_request.model = model;
            /// The `top_n` parameter is an upper bound, so a row may have fewer documents than it, but
            /// `AIRerankRequest::top_n` must not exceed the number of documents: cap it to the row's count.
            ai_rerank_request.top_n = std::min<UInt64>(top_n, docs_end - docs_begin);
            ai_rerank_request.function_name = getName();

            AIRerankResponse ai_rerank_response;
            bool success = false;

            for (UInt64 attempt = 0; attempt <= max_retries; ++attempt)
            {
                /// Reserve an API-call slot before each request; this also performs a quota check.
                /// Kept outside the `try` so a `throw_on_quota_exceeded` exception isn't caught by the retry handler.
                if (!quota_tracker->recordApiCall())
                    break;

                try
                {
                    ++total_api_calls;
                    /// Recorded even if `rerank` throws: the provider fills the billed usage before validating
                    /// the results, so a malformed response that was billed for is still counted. Cohere bills
                    /// in `search_units` and reports no tokens, in which case both token counts are `0`.
                    SCOPE_EXIT({
                        quota_tracker->recordTokens(ai_rerank_response.input_tokens, ai_rerank_response.output_tokens);
                        total_input_tokens += ai_rerank_response.input_tokens;
                        total_output_tokens += ai_rerank_response.output_tokens;
                        total_search_units += ai_rerank_response.search_units;
                    });
                    provider->rerank(ai_rerank_request, timeouts, ai_rerank_response);
                    success = true;
                    break;
                }
                catch (...)
                {
                    if (attempt < max_retries && FunctionBaseAI::isRetriableProviderError(std::current_exception()))
                    {
                        std::this_thread::sleep_for(std::chrono::milliseconds(FunctionBaseAI::computeRetryBackoffMs(retry_delay_ms, attempt)));
                        continue;
                    }

                    if (!throw_on_error)
                        break;

                    throw;
                }
            }

            if (success)
            {
                for (const auto & result : ai_rerank_response.results)
                {
                    index_col->insertValue(result.index + 1); /// 1-based, so `documents[t.index]` works directly
                    score_col->insertValue(result.relevance_score);
                }
                current_offset += ai_rerank_response.results.size();
                ++rows_processed;
            }
            else
                ++rows_skipped;

            offsets.push_back(current_offset);
        }

        return ColumnArray::create(ColumnTuple::create(Columns{std::move(index_col), std::move(score_col)}), std::move(offsets_col));
    }

private:
    static constexpr size_t query_arg_index = 0;
    static constexpr size_t documents_arg_index = 1;

    /// Parameters accepted in the optional trailing `Map(String, String)` argument. This is the complete
    /// spec passed to `resolveAIParams`, not extras merged with `FunctionBaseAI::commonParams`.
    /// `model` resolves map override -> named collection -> required. Letting the named collection
    /// supply a default is safe because a rerank score is consumed within a single query rather than
    /// persisted and compared across calls. `top_n` limits how many ranked results the provider
    /// returns; `0` (default) means all documents.
    static AIParamSpecs paramSpecs()
    {
        return {
            {"credentials", AIParamKind::String, std::nullopt},
            /// `model` is required, but is normally supplied by the named collection (same as `commonParams`).
            {"model", AIParamKind::String, std::nullopt, /*inherit_from_collection=*/ true},
            {"top_n", AIParamKind::UInt, Field(UInt64(0))},
        };
    }

    ContextPtr context;
    ContextPtr getContext() const { return context; }
};

}

REGISTER_FUNCTION(AiRerank)
{
    factory.registerFunction<FunctionAiRerank>(FunctionDocumentation{
        .description = R"(
Reranks a list of documents by semantic relevance to a query, using the configured reranking provider
(currently [Cohere's Rerank API](https://docs.cohere.com/reference/rerank)).

This function is experimental. Set `allow_experimental_ai_rerank_function = 1` to enable it.

Each row sends one request: `documents` and `query` are per-row, and the documents of one row are
never batched with another row's, since they must be ranked together against that row's query.

Returns an array of `(index, relevance_score)` tuples, ordered by descending relevance and truncated
to `top_n`. `index` is the (1-based) position of the document in the input `documents` array, so
`documents[t.index]` recovers the original text.

Credentials (a named collection specifying the provider, endpoint, model, and optionally an API key)
are taken from the `credentials` key of the parameter map, or from the
`ai_function_rerank_default_credentials` setting when the map omits it. Note that `aiRerank` uses a
separate default-credentials setting from the text and embedding functions, since a reranking
endpoint differs from both.

`model` is taken from the `model` key of the parameter map, falling back to the named collection's
`model`.

The optional `top_n` parameter limits the number of returned results to the `top_n` most relevant
documents; omitted or `0` returns all documents ranked, as does a `top_n` larger than the number of
documents.
)",
        .syntax = "aiRerank(query, documents[, params])",
        .arguments
        = {{"query", "The query to rank documents against.", {"String"}},
           {"documents", "Documents to rank.", {"Array(String)"}},
           {"params", "Optional constant `Map(String, String)` of parameters. Function-specific key: `top_n` (maximum number of ranked results to return; `0` or omitted returns all documents). The common parameters `credentials` and `model` also apply (see [AI Functions](/reference/functions/regular-functions/ai-functions)).", {"Map(String, String)"}}},
        .returned_value = {"Ranked `(index, relevance_score)` tuples, most relevant first, or an empty array if the query is NULL or empty, `documents` is empty, the request failed and `ai_function_throw_on_error` is disabled, or a quota was exceeded with `ai_function_throw_on_quota_exceeded` disabled.", {"Array(Tuple(index UInt32, relevance_score Float32))"}},
        .examples
        = {{"Rerank a constant list of documents (`credentials` can be omitted if the `ai_function_rerank_default_credentials` setting is set; `model` can be omitted if the named collection defines one)", "SET allow_experimental_ai_rerank_function = 1;\nSELECT aiRerank('capital of France', ['Berlin is in Germany.', 'Paris is the capital of France.'], map('credentials', 'ai_rerank_credentials', 'model', 'rerank-v3.5'))", "[(2, 0.98), (1, 0.01)]"},
           {"Limit to the top 2 results", "SELECT aiRerank('capital of France', documents, map('credentials', 'ai_rerank_credentials', 'top_n', '2')) FROM t", ""},
           {"Map ranked results back to the original documents", "SELECT arrayMap(t -> (documents[t.index], t.relevance_score), aiRerank('capital of France', documents, map('credentials', 'ai_rerank_credentials'))) FROM t", ""}},
        .introduced_in = {26, 10},
        .category = FunctionDocumentation::Category::AI});

    factory.registerAlias("AIRerank", "aiRerank");
}

}
