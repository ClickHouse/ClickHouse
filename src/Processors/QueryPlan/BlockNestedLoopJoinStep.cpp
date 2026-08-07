#include <Core/Block.h>
#include <Interpreters/ExpressionActions.h>
#include <IO/Operators.h>
#include <Processors/QueryPlan/BlockNestedLoopJoinStep.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Common/JSONBuilder.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

bool BlockNestedLoopJoinStep::isSupportedJoinType(JoinKind kind, JoinStrictness strictness)
{
    /// ASOF and PASTE prescribe the shape of the join condition (one inequality, or none at all),
    /// so an arbitrary predicate is not a condition they can express.
    if (strictness == JoinStrictness::Asof || isPaste(kind))
        return false;

    switch (strictness)
    {
        case JoinStrictness::All:
        case JoinStrictness::Any:
        case JoinStrictness::RightAny:
            return isInner(kind) || isLeftOrRight(kind) || isFull(kind) || isCrossOrComma(kind);
        case JoinStrictness::Semi:
        case JoinStrictness::Anti:
            return isLeftOrRight(kind);
        default:
            return false;
    }
}

BlockNestedLoopJoinStep::BlockNestedLoopJoinStep(
    const SharedHeader & left_header_,
    const SharedHeader & right_header_,
    ExpressionActionsPtr predicate_,
    JoinKind kind_,
    JoinStrictness strictness_,
    const SizeLimits & size_limits_,
    size_t max_block_size_,
    size_t max_block_bytes_)
    : kind(kind_)
    , strictness(strictness_)
    , size_limits(size_limits_)
    , max_block_size(max_block_size_)
    , max_block_bytes(max_block_bytes_)
{
    if (!isSupportedJoinType(kind, strictness))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Block nested loop join does not support {} {} JOIN",
            toString(strictness), toString(kind));

    if (!predicate_)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Block nested loop join requires a join condition");

    const auto & sample = predicate_->getSampleBlock();
    if (sample.columns() != 1 || !sample.getByPosition(0).type->canBeUsedInBooleanContext())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Block nested loop join condition must have a single boolean output, got {}",
            sample.dumpStructure());

    predicate.actions = std::move(predicate_);
    for (const auto & required_column : predicate.actions->getRequiredColumnsWithTypes())
    {
        bool in_left = left_header_->has(required_column.name);
        bool in_right = right_header_->has(required_column.name);
        if (in_left == in_right)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Block nested loop join condition input {} must come from exactly one input, found in {}",
                required_column.name, in_left ? "both" : "neither");
        if (in_left)
            predicate.inputs.push_back({.side = 0, .position = left_header_->getPositionByName(required_column.name)});
        else
            predicate.inputs.push_back({.side = 1, .position = right_header_->getPositionByName(required_column.name)});
    }

    updateInputHeaders({left_header_, right_header_});
}

/// The concatenation of the input columns: what the join operator itself outputs.
static SharedHeader concatHeaders(const SharedHeaders & headers)
{
    Block result;
    for (const auto & header : headers)
        for (const auto & column : *header)
            result.insert(ColumnWithTypeAndName(column.type->createColumn(), column.type, column.name));
    return std::make_shared<const Block>(std::move(result));
}

QueryPipelineBuilderPtr BlockNestedLoopJoinStep::updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings &)
{
    if (pipelines.size() != 2)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "BlockNestedLoopJoinStep expects two input pipelines, got {}", pipelines.size());

    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Block nested loop join is not implemented");
}

void BlockNestedLoopJoinStep::updateOutputHeader()
{
    output_header = concatHeaders(input_headers);
}

void BlockNestedLoopJoinStep::describeActions(FormatSettings & settings) const
{
    settings.out << settings.detail_prefix << "Type: " << toString(kind) << '\n';
    settings.out << settings.detail_prefix << "Strictness: " << toString(strictness) << '\n';
    settings.out << settings.detail_prefix << "Condition: " << predicate.actions->getSampleBlock().getByPosition(0).name << '\n';
}

void BlockNestedLoopJoinStep::describeActions(JSONBuilder::JSONMap & map) const
{
    map.add("Type", toString(kind));
    map.add("Strictness", toString(strictness));
    map.add("Condition", predicate.actions->getSampleBlock().getByPosition(0).name);
}

void BlockNestedLoopJoinStep::describePipeline(FormatSettings & settings) const
{
    IQueryPlanStep::describePipeline(processors, settings);
}

}
