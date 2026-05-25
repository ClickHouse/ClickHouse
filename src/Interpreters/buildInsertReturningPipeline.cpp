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
#include <Processors/Sources/DelayedSource.h>
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

    auto returning_context = makeReturningSelectContext(returning_select, context);
    auto insert_pipeline_holder = std::make_shared<QueryPipeline>(std::move(insert_pipeline));

    SharedHeader returning_header;
    {
        const auto select_query_options = SelectQueryOptions(QueryProcessingStage::Complete);
        if (returning_context->getSettingsRef()[Setting::allow_experimental_analyzer])
            returning_header = InterpreterSelectQueryAnalyzer::getSampleBlock(returning_select, returning_context, select_query_options);
        else
            returning_header = InterpreterSelectWithUnionQuery::getSampleBlock(returning_select, returning_context);
    }

    DelayedSource::Creator creator = [insert_pipeline_holder, returning_select, returning_context]() -> QueryPipelineBuilder
    {
        /// The INSERT pipeline is captured before executeQueryImpl wires process list / progress
        /// onto the outer RETURNING pipeline; attach them here so timeout/cancel work during INSERT.
        insert_pipeline_holder->setProcessListElement(returning_context->getProcessListElement());
        insert_pipeline_holder->setProgressCallback(returning_context->getProgressCallback());

        CompletedPipelineExecutor insert_executor(*insert_pipeline_holder);
        insert_executor.execute();

        const auto select_query_options = SelectQueryOptions(QueryProcessingStage::Complete);
        if (returning_context->getSettingsRef()[Setting::allow_experimental_analyzer])
        {
            InterpreterSelectQueryAnalyzer interpreter(returning_select, returning_context, select_query_options);
            return interpreter.buildQueryPipeline();
        }

        InterpreterSelectWithUnionQuery interpreter(returning_select, returning_context, select_query_options);
        return interpreter.buildQueryPipeline();
    };

    /// Preserve totals/extremes streams from the trailing SELECT. If DelayedSource
    /// omits a port that the inner pipeline produces, synchronizePorts drops it into NullSink.
    const bool add_totals_port = true;
    const bool add_extremes_port = true;

    Pipe pipe = createDelayedPipe(returning_header, std::move(creator), add_totals_port, add_extremes_port);
    return QueryPipeline(std::move(pipe));
}

}
