#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionBaseAI.h>
#include <Functions/AI/IAIProvider.h>
#include <Functions/AI/AIQuotaTracker.h>

#include <Common/ProfileEvents.h>
#include <Common/Exception.h>
#include <Common/UnorderedMapWithMemoryTracking.h>
#include <Common/VectorWithMemoryTracking.h>
#include <base/scope_guard.h>

#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnNullable.h>

#include <DataTypes/DataTypeNullable.h>
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
    extern const SettingsNonZeroUInt64 ai_function_rerank_max_batch_size;
    extern const SettingsBool allow_experimental_ai_relevance_function;
}

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int SUPPORT_IS_DISABLED;
}

namespace
{

class FunctionAiRelevance final : public IFunction
{
public:
    static constexpr auto name = "aiRelevance";

    static FunctionPtr create(ContextPtr context)
    {
        if (!context->getSettingsRef()[Setting::allow_experimental_ai_relevance_function])
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                "Function '{}' is experimental. Set `allow_experimental_ai_relevance_function` setting to enable it", name);

        return std::make_shared<FunctionAiRelevance>(context);
    }

    explicit FunctionAiRelevance(ContextPtr context_) : context(context_) {}

    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    /// func has side effects, f.e. each call updates quota, makes potentially expensive outside call, etc.
    bool isStateful() const override { return true; }

    /// Rerankers are not guaranteed to return the same score for the same input across calls (model
    /// updates behind the same name, provider-side nondeterminism), so results must not be reused across
    /// queries. Within a query, folding calls with identical args together is preferable: it keeps the
    /// results consistent and avoids paying for duplicate API calls.
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
            {"document", static_cast<FunctionArgumentDescriptor::TypeValidator>(&FunctionBaseAI::isStringOrNullableString), nullptr, "String"},
        };
        FunctionArgumentDescriptors optional_args{
            {"params", static_cast<FunctionArgumentDescriptor::TypeValidator>(&FunctionBaseAI::isStringToStringMap), &isColumnConst, "const Map(String, String)"},
        };
        validateFunctionArguments(*this, arguments, mandatory_args, optional_args);

        /// Always Nullable. The score is NULL when the query or document is NULL/empty, or the row was
        /// skipped (quota/error).
        return makeNullable(std::make_shared<DataTypeFloat32>());
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const auto & settings = getContext()->getSettingsRef();
        auto params = FunctionBaseAI::resolveAIParams(
            getContext(), arguments, paramSpecs(), settings[Setting::ai_function_rerank_default_credentials]);

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
        size_t max_batch_size = static_cast<size_t>(settings[Setting::ai_function_rerank_max_batch_size].value);

        /// Shared across every AI function call in the query
        auto quota_tracker = getContext()->getAIQuotaTracker();

        auto timeouts = ConnectionTimeouts::getHTTPTimeouts(settings, getContext()->getServerSettings());
        timeouts.receive_timeout = Poco::Timespan(static_cast<int64_t>(timeout_sec) /*s*/, 0 /*us*/);

        /// `isNullAt` and `getDataAt` are virtual on `IColumn`, so a single path covers `ColumnString`,
        /// `ColumnConst(ColumnString)`, `ColumnNullable` and `ColumnConst(ColumnNullable)` (e.g.
        /// `NULL::Nullable(String)`). A constant operand is read at index 0 rather than materialized.
        const IColumn & query_column = *arguments[query_arg_index].column;
        const IColumn & document_column = *arguments[document_arg_index].column;

        /// A row needs scoring only when both operands are non-null and non-empty; otherwise it is NULL.
        /// The `isNullAt` check also guards the only case where `ColumnNullable::getDataAt` throws (a NULL value).
        auto get_value = [](const IColumn & column, size_t row, std::string_view & out) -> bool
        {
            if (column.isNullAt(row))
                return false;
            out = column.getDataAt(row);
            return !out.empty();
        };

        /// Group the rows to score by query, in order of first appearance: one request ranks many
        /// documents against a single query. This relies on the reranker scoring each (query, document)
        /// pair independently of the other documents in the request, so scores from different requests
        /// (batches, blocks, threads) are comparable. The common case is a constant query, i.e. one group.
        struct QueryGroup
        {
            std::string_view query;
            VectorWithMemoryTracking<size_t> rows;
        };
        VectorWithMemoryTracking<QueryGroup> groups;
        UnorderedMapWithMemoryTracking<std::string_view, size_t> group_by_query;

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            std::string_view query;
            std::string_view document;
            if (!get_value(query_column, i, query) || !get_value(document_column, i, document))
                continue;

            auto [it, inserted] = group_by_query.try_emplace(query, groups.size());
            if (inserted)
                groups.push_back(QueryGroup{.query = query, .rows = {}});
            groups[it->second].rows.push_back(i);
        }

        auto score_col = ColumnFloat32::create(input_rows_count, 0.0f);
        auto null_map_col = ColumnUInt8::create(input_rows_count, static_cast<UInt8>(1));
        auto & scores = score_col->getData();
        auto & null_map = null_map_col->getData();

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

        bool quota_exceeded = false;
        for (const auto & group : groups)
        {
            for (size_t batch_start = 0; batch_start < group.rows.size(); batch_start += max_batch_size)
            {
                /// Once a quota is exceeded, every remaining row of every group is skipped.
                if (quota_exceeded || quota_tracker->checkQuotas())
                {
                    quota_exceeded = true;
                    rows_skipped += group.rows.size() - batch_start;
                    break;
                }

                size_t batch_end = std::min(batch_start + max_batch_size, group.rows.size());

                AIRerankRequest ai_rerank_request;
                ai_rerank_request.query = String(group.query);
                ai_rerank_request.model = model;
                ai_rerank_request.function_name = getName();
                ai_rerank_request.documents.reserve(batch_end - batch_start);
                for (size_t k = batch_start; k < batch_end; ++k)
                    ai_rerank_request.documents.push_back(document_column.getDataAt(group.rows[k]));

                AIRerankResponse ai_rerank_response;
                bool batch_ok = false;

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
                        batch_ok = true;
                        break;
                    }
                    catch (...)
                    {
                        if (attempt < max_retries && FunctionBaseAI::isRetriableProviderError(std::current_exception()))
                        {
                            std::this_thread::sleep_for(std::chrono::milliseconds(FunctionBaseAI::computeRetryBackoffMs(retry_delay_ms, attempt)));
                            continue;
                        }

                        if (!throw_on_error) /// Skip to next batch, this batch's rows stay NULL.
                            break;

                        throw;
                    }
                }

                if (!batch_ok)
                {
                    rows_skipped += batch_end - batch_start;
                    continue;
                }

                /// The provider guarantees one result per document, each with a distinct in-range `index`.
                for (const auto & result : ai_rerank_response.results)
                {
                    size_t row = group.rows[batch_start + result.index];
                    scores[row] = result.relevance_score;
                    null_map[row] = 0;
                }
                rows_processed += batch_end - batch_start;
            }
        }

        return ColumnNullable::create(std::move(score_col), std::move(null_map_col));
    }

private:
    static constexpr size_t query_arg_index = 0;
    static constexpr size_t document_arg_index = 1;

    /// Parameters accepted in the optional trailing `Map(String, String)` argument. This is the complete
    /// spec passed to `resolveAIParams`, not extras merged with `FunctionBaseAI::commonParams`.
    /// `model` resolves map override -> named collection -> required. Letting the named collection
    /// supply a default is safe because a relevance score is consumed within a single query rather than
    /// persisted and compared across calls.
    static AIParamSpecs paramSpecs()
    {
        return {
            {"credentials", AIParamKind::String, std::nullopt},
            /// `model` is required, but is normally supplied by the named collection (same as `commonParams`).
            {"model", AIParamKind::String, std::nullopt, /*inherit_from_collection=*/ true},
        };
    }

    ContextPtr context;
    ContextPtr getContext() const { return context; }
};

}

REGISTER_FUNCTION(AiRelevance)
{
    factory.registerFunction<FunctionAiRelevance>(FunctionDocumentation{
        .description = R"(
Scores the relevance of a document to a query using the configured reranking model (currently
[Cohere's Rerank API](https://docs.cohere.com/reference/rerank)). Use it to rerank candidate rows with
`ORDER BY aiRelevance(query, document) DESC LIMIT n`.

This function is experimental. Set `allow_experimental_ai_relevance_function = 1` to enable it.

Unlike [`aiSimilarity`](#aiSimilarity), which compares the embeddings of two texts symmetrically,
`aiRelevance` is asymmetric: it scores how well `document` answers `query`, using a cross-encoder model
that reads both texts together. This is usually more accurate for search, but costs one reranking
request per batch of documents instead of reusable embeddings.

Within a single block of rows, documents scored against the same query are grouped into batches of up to
[`ai_function_rerank_max_batch_size`](/reference/settings/session-settings/ai-function#ai_function_rerank_max_batch_size)
documents per HTTP request. The query is usually a constant, in which case every row of the block shares
a request. Scores from different requests are compared directly (for example by `ORDER BY`), which relies
on the reranker scoring each (query, document) pair independently of the other documents in the request,
as cross-encoder rerankers do.

Every row that reaches the function is sent to the provider, so narrow down the candidates first
(for example with a `WHERE` condition, a full-text search or a vector search): `ORDER BY ... LIMIT` does
not reduce the number of rows scored.

Credentials (a named collection specifying the provider, endpoint, model, and optionally an API key)
are taken from the `credentials` key of the parameter map, or from the
`ai_function_rerank_default_credentials` setting when the map omits it. Note that `aiRelevance` uses a
separate default-credentials setting from the text and embedding functions, since a reranking
endpoint differs from both.

`model` is taken from the `model` key of the parameter map, falling back to the named collection's
`model`.
)",
        .syntax = "aiRelevance(query, document[, params])",
        .arguments
        = {{"query", "The query to score the document against.", {"String"}},
           {"document", "The document to score.", {"String"}},
           {"params", "Optional constant `Map(String, String)` of parameters. The common parameters `credentials` and `model` apply (see [AI Functions](/reference/functions/regular-functions/ai-functions)).", {"Map(String, String)"}}},
        .returned_value = {"The relevance score in `[0, 1]` (higher is more relevant), or NULL if the query or document is NULL or empty, the request failed and `ai_function_throw_on_error` is disabled, or a quota was exceeded with `ai_function_throw_on_quota_exceeded` disabled.", {"Nullable(Float32)"}},
        .examples
        = {{"Rerank the candidates retrieved by a filter and return the top 2 rows (`credentials` can be omitted if the `ai_function_rerank_default_credentials` setting is set; `model` can be omitted if the named collection defines one)", "SET allow_experimental_ai_relevance_function = 1;\nCREATE TABLE docs (url String, category String, text String) ENGINE = Memory;\nINSERT INTO docs VALUES ('https://example.com/paris', 'geography', 'Paris is the capital of France'), ('https://example.com/berlin', 'geography', 'Berlin is the capital of Germany'), ('https://example.com/eiffel', 'geography', 'The Eiffel Tower stands in France'), ('https://example.com/baguette', 'cooking', 'A baguette is the pride of France');\nSELECT url, text, aiRelevance('capital of France', text, map('credentials', 'ai_rerank_credentials', 'model', 'rerank-v3.5')) AS score\nFROM docs\nWHERE category = 'geography'\nORDER BY score DESC\nLIMIT 2", ""}},
        .introduced_in = {26, 10},
        .category = FunctionDocumentation::Category::AI});

    factory.registerAlias("AIRelevance", "aiRelevance");
}

}
