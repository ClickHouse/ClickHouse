#include <Interpreters/buildInsertReturningPipeline.h>

#include <Common/Exception.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterSetQuery.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Parsers/ASTInsertQuery.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <QueryPipeline/StreamLocalLimits.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

ContextMutablePtr makeReturningSelectContext(const ASTPtr & returning_select, ContextPtr context)
{
    auto returning_context = Context::createCopy(context);
    InterpreterSetQuery::applySettingsFromQuery(returning_select, returning_context);
    return returning_context;
}

namespace Setting
{
    extern const SettingsBool allow_experimental_analyzer;
    extern const SettingsUInt64 max_result_rows;
    extern const SettingsUInt64 max_result_bytes;
    extern const SettingsOverflowMode result_overflow_mode;
    extern const SettingsSeconds max_execution_time;
    extern const SettingsOverflowMode timeout_overflow_mode;
}

QueryPipeline buildReturningSelectPipeline(const ASTPtr & returning_select, ContextPtr context)
{
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
        /// The INSERT and RETURNING phases share one query (and `ProcessListElement`), whose time limits were
        /// captured from the INSERT-phase settings. Enforce the RETURNING subquery's `max_execution_time` on the
        /// result pipeline explicitly so `RETURNING (SELECT ... SETTINGS max_execution_time=...)` is honored.
        limits.speed_limits.max_execution_time = settings[Setting::max_execution_time];
        limits.timeout_overflow_mode = settings[Setting::timeout_overflow_mode];
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
