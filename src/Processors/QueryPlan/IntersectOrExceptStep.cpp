#include <Processors/QueryPlan/IntersectOrExceptStep.h>

#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnConst.h>
#include <Columns/IColumn.h>
#include <Common/assert_cast.h>
#include <Core/Block.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/IntersectOrExceptTransform.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Processors/ResizeProcessor.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <base/EnumReflection.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_DATA;
    extern const int SUPPORT_IS_DISABLED;
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

QueryPipelineBuilderPtr IntersectOrExceptStep::updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings & settings)
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

    /// Zero means the step was deserialized on a worker; use the executing server's own setting.
    size_t new_max_threads = max_threads ? max_threads : settings.max_threads;
    *pipeline = QueryPipelineBuilder::unitePipelines(std::move(pipelines), new_max_threads, &processors);
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

/// First query-plan serialization version that registers the "IntersectOrExcept" step.
static constexpr auto MIN_SERIALIZATION_VERSION_WITH_INTERSECT_OR_EXCEPT_STEP = 14;

void IntersectOrExceptStep::serialize(Serialization & ctx) const
{
    /// Throw rather than send a step name an older peer does not know.
    if (ctx.version < MIN_SERIALIZATION_VERSION_WITH_INTERSECT_OR_EXCEPT_STEP)
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
            "make_distributed_plan: serializing an IntersectOrExceptStep requires query plan serialization "
            "version >= {}; all nodes must run the same version", MIN_SERIALIZATION_VERSION_WITH_INTERSECT_OR_EXCEPT_STEP);

    /// `max_threads` is not serialized: zero makes `updatePipeline` use the executing server's own setting.
    writeIntBinary(static_cast<UInt8>(current_operator), ctx.out);
}

QueryPlanStepPtr IntersectOrExceptStep::deserialize(Deserialization & ctx)
{
    /// Mirrors the guard in `serialize`: a peer below this version cannot have written this step.
    if (ctx.version < MIN_SERIALIZATION_VERSION_WITH_INTERSECT_OR_EXCEPT_STEP)
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
            "make_distributed_plan: deserializing an IntersectOrExceptStep requires query plan serialization "
            "version >= {}; all nodes must run the same version", MIN_SERIALIZATION_VERSION_WITH_INTERSECT_OR_EXCEPT_STEP);

    if (ctx.input_headers.size() != 2)
        throw Exception(ErrorCodes::INCORRECT_DATA, "IntersectOrExceptStep must have two input streams");

    UInt8 operator_value = 0;
    readIntBinary(operator_value, ctx.in);
    const auto current_operator = magic_enum::enum_cast<Operator>(operator_value);
    /// `UNKNOWN` is a member of the enum but not a valid operator, reject it too.
    if (!current_operator || *current_operator == Operator::UNKNOWN)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected intersect/except operator value {}", static_cast<UInt32>(operator_value));
    return std::make_unique<IntersectOrExceptStep>(ctx.input_headers, *current_operator, /*max_threads_=*/0);
}

void registerIntersectOrExceptStep(QueryPlanStepRegistry & registry);
void registerIntersectOrExceptStep(QueryPlanStepRegistry & registry)
{
    registry.registerStep("IntersectOrExcept", &IntersectOrExceptStep::deserialize);
}

}
