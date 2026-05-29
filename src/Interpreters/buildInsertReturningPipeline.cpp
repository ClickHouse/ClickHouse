#include <Interpreters/buildInsertReturningPipeline.h>

#include <Common/Exception.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterSetQuery.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <QueryPipeline/StreamLocalLimits.h>

#include <string_view>
#include <unordered_set>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

namespace
{
    /// The INSERT and RETURNING phases share one query, one `ProcessListElement` and one thread-group
    /// `MemoryTracker`, all set up once by `ProcessList::insert` from the outer INSERT-phase settings. Query-global
    /// resource and execution limits therefore cannot be re-applied for the RETURNING phase: memory limits would have
    /// no effect, and the query time limit is measured from INSERT registration (so it cannot bound the subquery
    /// alone). Rather than silently ignore such settings when they appear in the RETURNING subquery's `SETTINGS`
    /// clause, reject them explicitly. Settings enforceable on the result pipeline (`max_result_rows`,
    /// `max_result_bytes`, `result_overflow_mode`) remain supported.
    void rejectUnsupportedReturningSettings(const ASTPtr & returning_select)
    {
        static const std::unordered_set<std::string_view> unsupported_settings = {
            "max_memory_usage",
            "max_memory_usage_for_user",
            "max_execution_time",
            "timeout_overflow_mode",
        };

        const auto * select_with_union = returning_select->as<ASTSelectWithUnionQuery>();
        if (!select_with_union || !select_with_union->list_of_selects)
            return;

        const auto & selects = select_with_union->list_of_selects->children;
        if (selects.empty())
            return;

        /// Only the last `SELECT`'s `SETTINGS` are applied to the subquery context (see `applySettingsFromQuery`).
        const auto * last_select = selects.back()->as<ASTSelectQuery>();
        if (!last_select || !last_select->settings())
            return;

        for (const auto & change : last_select->settings()->as<ASTSetQuery &>().changes)
        {
            if (unsupported_settings.contains(change.name))
                throw Exception(
                    ErrorCodes::NOT_IMPLEMENTED,
                    "Setting '{}' is not supported in the SETTINGS clause of an INSERT ... RETURNING subquery",
                    change.name);
        }
    }
}

ContextMutablePtr makeReturningSelectContext(const ASTPtr & returning_select, ContextPtr context)
{
    auto returning_context = Context::createCopy(context);
    /// `Context::createCopy` gives the subquery an independent `QueryAccessInfo`, so the tables and columns read by
    /// the RETURNING subquery would be missing from `system.query_log`. Share the outer query's access info (the same
    /// approach as the materialized-view path in `InsertDependenciesBuilder`) so the accesses are recorded.
    returning_context->setQueryAccessInfo(context->getQueryAccessInfoPtr());
    InterpreterSetQuery::applySettingsFromQuery(returning_select, returning_context);
    return returning_context;
}

namespace Setting
{
    extern const SettingsBool allow_experimental_analyzer;
    extern const SettingsUInt64 max_result_rows;
    extern const SettingsUInt64 max_result_bytes;
    extern const SettingsOverflowMode result_overflow_mode;
}

QueryPipeline buildReturningSelectPipeline(const ASTPtr & returning_select, ContextPtr context)
{
    rejectUnsupportedReturningSettings(returning_select);
    auto returning_context = makeReturningSelectContext(returning_select, context);
    const auto select_query_options = SelectQueryOptions(QueryProcessingStage::Complete);
    if (returning_context->getSettingsRef()[Setting::allow_experimental_analyzer])
    {
        InterpreterSelectQueryAnalyzer interpreter(returning_select, returning_context, select_query_options);
        return QueryPipelineBuilder::getPipeline(interpreter.buildQueryPipeline());
    }

    InterpreterSelectWithUnionQuery interpreter(returning_select, returning_context, select_query_options);
    return QueryPipelineBuilder::getPipeline(interpreter.buildQueryPipeline());
}

void setupPullingQueryPipeline(
    QueryPipeline & pipeline,
    ContextPtr context,
    QueryProcessingStage::Enum stage,
    const ASTPtr & returning_select)
{
    pipeline.setProgressCallback(context->getProgressCallback());
    pipeline.setProcessListElement(context->getProcessListElement());

    if (stage == QueryProcessingStage::Complete && pipeline.pulling())
    {
        const auto limits_context = returning_select ? makeReturningSelectContext(returning_select, context) : context;
        const auto & settings = limits_context->getSettingsRef();
        StreamLocalLimits limits;
        limits.mode = LimitsMode::LIMITS_CURRENT;
        limits.size_limits = SizeLimits(
            settings[Setting::max_result_rows],
            settings[Setting::max_result_bytes],
            settings[Setting::result_overflow_mode]);
        pipeline.setLimitsAndQuota(limits, context->getQuota());
    }
}

bool replacePipelineWithInsertReturningAfterPush(
    BlockIO & io,
    const ASTInsertQuery & insert_query,
    ContextPtr context,
    QueryProcessingStage::Enum stage)
{
    if (!insert_query.returning_select)
        return false;

    io.pipeline.reset();
    io.pipeline = buildReturningSelectPipeline(insert_query.returning_select, context);
    setupPullingQueryPipeline(io.pipeline, context, stage, insert_query.returning_select);
    if (io.finish_callback_state)
        io.finish_callback_state->insert_returning_result_as_select = true;
    return true;
}

QueryPipeline buildInsertReturningPipeline(
    QueryPipeline insert_pipeline,
    const ASTPtr & returning_select,
    ContextPtr context)
{
    if (insert_pipeline.pushing())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "INSERT pipeline must be completed before wrapping with RETURNING");

    /// Run INSERT to completion before building the RETURNING `SELECT` pipeline, so semantic errors during
    /// subquery planning (unknown identifiers, etc.) do not happen before persisted insert side effects — same ordering
    /// as the native-protocol push path (`replacePipelineWithInsertReturningAfterPush`).
    insert_pipeline.setProcessListElement(context->getProcessListElement());
    insert_pipeline.setProgressCallback(context->getProgressCallback());

    CompletedPipelineExecutor insert_executor(insert_pipeline);
    insert_executor.execute();

    return buildReturningSelectPipeline(returning_select, context);
}

}
