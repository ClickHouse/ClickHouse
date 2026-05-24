#include <Interpreters/buildInsertReturningPipeline.h>

#include <Common/Exception.h>
#include <Core/Settings.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/Sources/DelayedSource.h>
#include <QueryPipeline/QueryPipelineBuilder.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace Setting
{
    extern const SettingsBool allow_experimental_analyzer;
}

QueryPipeline buildInsertReturningPipeline(
    QueryPipeline insert_pipeline,
    const ASTPtr & returning_select,
    ContextPtr context)
{
    if (insert_pipeline.pushing())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "INSERT pipeline must be completed before wrapping with RETURNING");

    auto insert_pipeline_holder = std::make_shared<QueryPipeline>(std::move(insert_pipeline));

    SharedHeader returning_header;
    {
        const auto select_query_options = SelectQueryOptions(QueryProcessingStage::Complete);
        if (context->getSettingsRef()[Setting::allow_experimental_analyzer])
            returning_header = InterpreterSelectQueryAnalyzer::getSampleBlock(returning_select, context, select_query_options);
        else
            returning_header = InterpreterSelectWithUnionQuery::getSampleBlock(returning_select, context);
    }

    DelayedSource::Creator creator = [insert_pipeline_holder, returning_select, context]() -> QueryPipelineBuilder
    {
        CompletedPipelineExecutor insert_executor(*insert_pipeline_holder);
        insert_executor.execute();

        const auto select_query_options = SelectQueryOptions(QueryProcessingStage::Complete);
        if (context->getSettingsRef()[Setting::allow_experimental_analyzer])
        {
            InterpreterSelectQueryAnalyzer interpreter(returning_select, context, select_query_options);
            return interpreter.buildQueryPipeline();
        }

        InterpreterSelectWithUnionQuery interpreter(returning_select, context, select_query_options);
        return interpreter.buildQueryPipeline();
    };

    Pipe pipe = createDelayedPipe(returning_header, std::move(creator), false, false);
    return QueryPipeline(std::move(pipe));
}

}
