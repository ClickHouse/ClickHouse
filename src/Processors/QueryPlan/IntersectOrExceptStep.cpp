#include <Processors/QueryPlan/IntersectOrExceptStep.h>

#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnConst.h>
#include <Columns/IColumn.h>
#include <Common/assert_cast.h>
#include <Core/Block.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <QueryPipeline/scatterByPartition.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/IntersectOrExceptTransform.h>
#include <Processors/ResizeProcessor.h>

#include <numeric>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

static bool containsAggregateStateColumn(const IColumn & column)
{
    if (typeid_cast<const ColumnAggregateFunction *>(&column))
        return true;

    bool found = false;
    column.forEachSubcolumn([&](const auto & subcolumn) { found = found || containsAggregateStateColumn(*subcolumn); });
    return found;
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

    /// Build the common header following the same rule as getLeastSuperColumn: keep a
    /// column Const only when every branch is Const with the same value, otherwise
    /// materialize it. This matches the execution-time makeConvertingActions path, which
    /// can convert a branch to a full column but not to a different branch's Const value.
    ColumnsWithTypeAndName common = input_headers.front()->getColumnsWithTypeAndName();
    bool materialized = false;
    for (size_t col = 0; col < common.size(); ++col)
    {
        if (!common[col].column || !isColumnConst(*common[col].column))
            continue;

        /// Aggregate-state values cannot be compared as `Field`: the comparison throws when the
        /// aggregate function type names differ, and they may legitimately differ between branches
        /// when the functions have the same state representation (e.g. `quantileState` and
        /// `quantilesState(0.9)`). Don't keep constness for them, materialize instead.
        if (containsAggregateStateColumn(assert_cast<const ColumnConst &>(*common[col].column).getDataColumn()))
        {
            common[col].column = common[col].column->convertToFullColumnIfConst();
            materialized = true;
            continue;
        }

        const Field value = assert_cast<const ColumnConst &>(*common[col].column).getField();
        bool keep_const = true;
        for (const auto & header : input_headers)
        {
            const auto & branch = header->getByPosition(col).column;
            if (!branch || !isColumnConst(*branch) || assert_cast<const ColumnConst &>(*branch).getField() != value)
            {
                keep_const = false;
                break;
            }
        }

        if (!keep_const)
        {
            common[col].column = common[col].column->convertToFullColumnIfConst();
            materialized = true;
        }
    }

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

    size_t num_partitions = isPartitioned() ? max_threads : 1;
    /// Partitioning is an optimization: with a huge `max_threads` reduce the partition count instead
    /// of failing on the scatter connection limit. Any partition count keeps the output streams disjoint.
    for (const auto & cur_pipeline : pipelines)
        num_partitions = std::min(num_partitions, std::max<size_t>(1, scatter_connection_count_limit / cur_pipeline->getNumStreams()));

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

        QueryPipelineProcessorsCollector collector(*cur_pipeline, this);
        if (num_partitions == 1)
        {
            cur_pipeline->addTransform(std::make_shared<ResizeProcessor>(getOutputHeader(), cur_pipeline->getNumStreams(), 1));
        }
        else
        {
            /// Both branches have the identical header (types and constness) after the conversion
            /// above, so equal rows hash equally and land in the same partition on both sides. Each
            /// partition is then an independent, exact set operation on its subset of rows.
            ColumnNumbers key_columns(getOutputHeader()->columns());
            std::iota(key_columns.begin(), key_columns.end(), 0);
            scatterByPartition(*cur_pipeline, num_partitions, key_columns);
        }
        auto added_processors = collector.detachProcessors();
        processors.insert(processors.end(), added_processors.begin(), added_processors.end());
    }

    *pipeline = QueryPipelineBuilder::unitePipelines(std::move(pipelines), max_threads, &processors);

    if (num_partitions == 1)
    {
        auto transform = std::make_shared<IntersectOrExceptTransform>(getOutputHeader(), current_operator);
        processors.push_back(transform);
        pipeline->addTransform(std::move(transform));
        return pipeline;
    }

    /// The united ports are [left_0 .. left_{N-1}, right_0 .. right_{N-1}]: pair partition i of both sides.
    QueryPipelineProcessorsCollector collector(*pipeline, this);
    pipeline->transform([&](OutputPortRawPtrs ports)
    {
        chassert(ports.size() == 2 * num_partitions);
        Processors result;
        for (size_t i = 0; i < num_partitions; ++i)
        {
            auto transform = std::make_shared<IntersectOrExceptTransform>(getOutputHeader(), current_operator);
            connect(*ports[i], transform->getInputs().front());
            connect(*ports[num_partitions + i], transform->getInputs().back());
            result.push_back(std::move(transform));
        }
        return result;
    });
    auto added_processors = collector.detachProcessors();
    processors.insert(processors.end(), added_processors.begin(), added_processors.end());

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
