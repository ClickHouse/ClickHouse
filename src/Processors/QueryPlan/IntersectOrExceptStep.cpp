#include <Processors/QueryPlan/IntersectOrExceptStep.h>

#include <Columns/IColumn.h>
#include <Columns/getLeastSuperColumn.h>
#include <Core/Block.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/IntersectOrExceptTransform.h>
#include <Processors/ResizeProcessor.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

static SharedHeader checkHeaders(const SharedHeaders & input_headers)
{
    if (input_headers.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot perform intersect/except on empty set of query plan steps");

    /// Branches are optimized independently, so filter push-down may constant-fold a
    /// column in one branch but not its sibling. Tolerate exactly that: compare with the
    /// top-level Const stripped, which keeps the check otherwise strict (different types
    /// and divergent Sparse/Replicated wrappers are still rejected). The strip is guarded
    /// by isColumnConst because convertToFullColumnIfConst is broader for some columns
    /// (e.g. ColumnArray materializes const nested data too), and the conversion path at
    /// execution time only reconciles a top-level Const. Header columns may legitimately
    /// have a null column pointer (e.g. __grouping_set from WithMergeableState); leave
    /// those untouched, names and types are still validated below.
    auto without_top_level_const = [](const Block & header)
    {
        ColumnsWithTypeAndName columns = header.getColumnsWithTypeAndName();
        for (auto & column : columns)
            if (column.column && isColumnConst(*column.column))
                column.column = column.column->convertToFullColumnIfConst();
        return Block(std::move(columns));
    };

    Block reference = without_top_level_const(*input_headers.front());
    for (const auto & header : input_headers)
        assertBlocksHaveEqualStructure(without_top_level_const(*header), reference, "IntersectOrExceptStep");

    /// Build the common header following the same rule as getLeastSuperColumn. The columns were
    /// just asserted to match one by one, so branches are looked up by position.
    bool materialized = false;
    ColumnsWithTypeAndName common = reconcileConstness(
        input_headers.front()->getColumnsWithTypeAndName(),
        input_headers.size(),
        [&](size_t branch, size_t position, const String &) { return &input_headers[branch]->getByPosition(position); },
        &materialized);

    if (!materialized)
        return input_headers.front();
    return std::make_shared<const Block>(std::move(common));
}

IntersectOrExceptStep::IntersectOrExceptStep(
    SharedHeaders input_headers_, Operator operator_, size_t max_threads_)
    : current_operator(operator_)
    , max_threads(max_threads_)
{
    updateInputHeaders(std::move(input_headers_));
}

void IntersectOrExceptStep::updateOutputHeader()
{
    output_header = checkHeaders(input_headers);
}

QueryPipelineBuilderPtr IntersectOrExceptStep::updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings &)
{
    auto pipeline = std::make_unique<QueryPipelineBuilder>();

    if (pipelines.empty())
    {
        QueryPipelineProcessorsCollector collector(*pipeline, this);
        pipeline->init(Pipe(std::make_shared<NullSource>(output_header)));
        processors = collector.detachProcessors();
        return pipeline;
    }

    for (auto & cur_pipeline : pipelines)
    {
        /// The check must be strict about constness (blocksHaveEqualStructure, not
        /// isCompatibleHeader): when a branch constant-folds, the common header
        /// materializes the column, and the converting expression must be applied to
        /// every stream of the branch pipeline - including the totals and extremes
        /// ports, which addSimpleTransform covers but the main-stream processors
        /// added below do not. Otherwise a Const totals port survives next to full
        /// main streams and fails the per-stream structure check downstream.
        if (!blocksHaveEqualStructure(cur_pipeline->getHeader(), *getOutputHeader()))
        {
            QueryPipelineProcessorsCollector collector(*cur_pipeline, this);
            auto converting_dag = ActionsDAG::makeConvertingActions(
                cur_pipeline->getHeader().getColumnsWithTypeAndName(),
                getOutputHeader()->getColumnsWithTypeAndName(),
                ActionsDAG::MatchColumnsMode::Name,
                nullptr);

            auto converting_actions = std::make_shared<ExpressionActions>(std::move(converting_dag));
            cur_pipeline->addSimpleTransform([&](const SharedHeader & cur_header)
            {
                return std::make_shared<ExpressionTransform>(cur_header, converting_actions);
            });

            auto added_processors = collector.detachProcessors();
            processors.insert(processors.end(), added_processors.begin(), added_processors.end());
        }

        /// For the case of union.
        cur_pipeline->addTransform(std::make_shared<ResizeProcessor>(getOutputHeader(), cur_pipeline->getNumStreams(), 1));
    }

    *pipeline = QueryPipelineBuilder::unitePipelines(std::move(pipelines), max_threads, &processors);
    auto transform = std::make_shared<IntersectOrExceptTransform>(getOutputHeader(), current_operator);
    processors.push_back(transform);
    pipeline->addTransform(std::move(transform));

    return pipeline;
}

void IntersectOrExceptStep::describePipeline(FormatSettings & settings) const
{
    IQueryPlanStep::describePipeline(processors, settings);
}

QueryPlanStepPtr IntersectOrExceptStep::clone() const
{
    return std::make_unique<IntersectOrExceptStep>(*this);
}

}
