#pragma once

#include <Core/Joins.h>
#include <Interpreters/ExpressionActions.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <QueryPipeline/SizeLimits.h>

namespace DB
{

/// An arbitrary `JOIN ON` condition evaluated on (left row, right row) pairs inside the operator.
struct BlockNestedLoopPredicate
{
    /// Where a required column of `actions` comes from: the input (0 = left, 1 = right)
    /// and the column's position in that input's header.
    struct Source
    {
        size_t side = 0;
        size_t position = 0;
    };

    ExpressionActionsPtr actions;
    /// One entry per required column of `actions`, in `getRequiredColumnsWithTypes` order.
    std::vector<Source> inputs;
};

/// Joins two data streams by an arbitrary boolean `JOIN ON` condition with a block nested loop:
/// the right input is materialized, then every left row is matched against it by evaluating the
/// condition on tiles of candidate pairs. This is the last-resort operator, chosen when no other
/// algorithm can claim the condition, so it must support every join kind and strictness except
/// `ASOF` and `PASTE`, both of which carry their own required condition shape.
class BlockNestedLoopJoinStep : public IQueryPlanStep
{
public:
    BlockNestedLoopJoinStep(
        const SharedHeader & left_header_,
        const SharedHeader & right_header_,
        ExpressionActionsPtr predicate_,
        JoinKind kind_,
        JoinStrictness strictness_,
        const SizeLimits & size_limits_,
        size_t max_block_size_,
        size_t max_block_bytes_);

    /// Whether the step can execute this join type.
    static bool isSupportedJoinType(JoinKind kind, JoinStrictness strictness);

    String getName() const override { return "BlockNestedLoopJoin"; }

    QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings &) override;

    void describePipeline(FormatSettings & settings) const override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

private:
    void updateOutputHeader() override;

    /// The whole ON condition, with its inputs resolved against the input headers.
    BlockNestedLoopPredicate predicate;

    JoinKind kind;
    JoinStrictness strictness;

    /// Limits on the materialized right input, from `max_rows_in_join` / `max_bytes_in_join`.
    SizeLimits size_limits;
    /// Limits on a result block, from `max_block_size` / `max_joined_block_size_rows` and
    /// `max_joined_block_size_bytes`. Read by the operator, which is not built yet.
    [[maybe_unused]] size_t max_block_size;
    [[maybe_unused]] size_t max_block_bytes;
};

}
