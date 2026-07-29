#include <Columns/ColumnConst.h>
#include <Columns/IColumn.h>
#include <Common/NaNUtils.h>
#include <Common/assert_cast.h>
#include <Core/Block.h>
#include <Interpreters/ExpressionActions.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <base/defines.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int PARAMETER_OUT_OF_BOUND;
}

static SharedHeader checkHeaders(const SharedHeaders & input_headers)
{
    if (input_headers.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot unite an empty set of query plan steps");

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
        assertBlocksHaveEqualStructure(without_top_level_const(*header), reference, "UnionStep");

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

UnionStep::UnionStep(SharedHeaders input_headers_, size_t max_threads_, bool allow_narrowing_)
    : max_threads(max_threads_)
    , allow_narrowing(allow_narrowing_)
{
    updateInputHeaders(std::move(input_headers_));
}

void UnionStep::updateOutputHeader()
{
    output_header = checkHeaders(input_headers);
}

QueryPipelineBuilderPtr UnionStep::updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings & settings)
{
    auto pipeline = std::make_unique<QueryPipelineBuilder>();

    if (pipelines.empty())
    {
        QueryPipelineProcessorsCollector collector(*pipeline, this);
        pipeline->init(Pipe(std::make_shared<NullSource>(output_header)));
        processors = collector.detachProcessors();
        return pipeline;
    }

    size_t new_max_threads = max_threads ? max_threads : settings.max_threads;

    for (auto & cur_pipeline : pipelines)
    {
        /// Headers for union must be equal.
        /// But, just in case, convert it to the same header if not.
        /// This can happen when PREWHERE optimization adds extra pass-through columns
        /// to ReadFromMergeTree output that are not consumed by the expression DAG above,
        /// causing plan headers and pipeline headers to diverge.
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

#if defined(DEBUG_OR_SANITIZER_BUILD)
        assertCompatibleHeader(cur_pipeline->getHeader(), *getOutputHeader(), "UnionStep");
#endif
    }

    *pipeline = QueryPipelineBuilder::unitePipelines(std::move(pipelines), new_max_threads, &processors);

    /// The `max_streams_for_union_step*` cap only applies to steps built for SQL
    /// `UNION ALL` / `UNION DISTINCT`, and only while no downstream step relies on
    /// per-stream sortedness of the union output. For all other cases the narrowing
    /// must be skipped: shuffling streams through `ConcatProcessor` would break the
    /// ordering invariants of `GroupingAggregatedTransform` (memory-efficient
    /// distributed aggregation), `MergingSortedTransform`, and similar order-sensitive
    /// consumers. We still validate the ratio so misconfiguration is reported on every
    /// query rather than only when a narrowable `UNION` happens to be present.
    const double max_streams_ratio = settings.max_streams_for_union_step_to_max_threads_ratio;
    if (!isFinite(max_streams_ratio) || max_streams_ratio < 0)
        throw Exception(ErrorCodes::PARAMETER_OUT_OF_BOUND,
            "Invalid value for `max_streams_for_union_step_to_max_threads_ratio`: {}. Must be a finite non-negative number.",
            max_streams_ratio);

    if (!allow_narrowing)
        return pipeline;

    size_t effective_max_streams = settings.max_streams_for_union_step;
    if (max_streams_ratio > 0 && new_max_threads > 0)
    {
        double streams_with_ratio = static_cast<double>(new_max_threads) * max_streams_ratio;
        if (!canConvertTo<size_t>(streams_with_ratio))
            throw Exception(ErrorCodes::PARAMETER_OUT_OF_BOUND,
                "`max_streams_for_union_step_to_max_threads_ratio` produces an out-of-range stream limit "
                "(max_threads={}, ratio={}, product={}). Make sure the product fits in size_t.",
                new_max_threads, max_streams_ratio, streams_with_ratio);
        size_t max_streams_from_ratio = static_cast<size_t>(streams_with_ratio);
        if (max_streams_from_ratio == 0)
            max_streams_from_ratio = 1;
        if (effective_max_streams)
            effective_max_streams = std::min(effective_max_streams, max_streams_from_ratio);
        else
            effective_max_streams = max_streams_from_ratio;
    }

    if (effective_max_streams && pipeline->getNumStreams() > effective_max_streams)
    {
        QueryPipelineProcessorsCollector collector(*pipeline, this);
        pipeline->narrow(effective_max_streams);
        auto added_processors = collector.detachProcessors();
        processors.insert(processors.end(), added_processors.begin(), added_processors.end());
    }

    return pipeline;
}

void UnionStep::describePipeline(FormatSettings & settings) const
{
    IQueryPlanStep::describePipeline(processors, settings);
}

void UnionStep::serialize(Serialization & ctx) const
{
    (void)ctx;
}

QueryPlanStepPtr UnionStep::deserialize(Deserialization & ctx)
{
    return std::make_unique<UnionStep>(ctx.input_headers);
}

QueryPlanStepPtr UnionStep::clone() const
{
    return std::make_unique<UnionStep>(*this);
}

void registerUnionStep(QueryPlanStepRegistry & registry);
void registerUnionStep(QueryPlanStepRegistry & registry)
{
    registry.registerStep("Union", &UnionStep::deserialize);
}

}
