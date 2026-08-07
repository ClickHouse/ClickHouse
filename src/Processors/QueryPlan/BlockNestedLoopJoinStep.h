#pragma once

#include <Core/Joins.h>
#include <Interpreters/ExpressionActions.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/Transforms/BlockNestedLoopJoinTransform.h>
#include <QueryPipeline/SizeLimits.h>

namespace DB
{

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
        BlockNestedLoopStoreSettings store_settings_,
        size_t max_block_size_,
        size_t max_block_bytes_);

    /// The two stages of execution, kept apart so that `EXPLAIN ANALYZE` attributes the time
    /// of materializing the right input and the time of matching separately.
    enum class Stage : size_t
    {
        Build = 1,
        Probe = 2,
    };

    /// Whether the step can execute this join type.
    static bool isSupportedJoinType(JoinKind kind, JoinStrictness strictness);

    String getName() const override { return "BlockNestedLoopJoin"; }

    QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings &) override;

    void describePipeline(FormatSettings & settings) const override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    std::vector<size_t> getStepGroups() const override;
    String getStepGroupName(size_t group) const override;

private:
    void updateOutputHeader() override;

    /// Appends the stage that emits the build rows no probe row matched, held back until every probe
    /// stream has finished.
    void addUnmatchedBuildRowsStage(
        QueryPipelineBuilder & pipeline, const BlockNestedLoopJoinDataPtr & data, size_t max_streams) const;

    /// The whole ON condition, with its inputs resolved against the input headers.
    BlockNestedLoopPredicate predicate;

    JoinKind kind;
    JoinStrictness strictness;

    /// Limits on the materialized right input, from `max_rows_in_join` / `max_bytes_in_join`.
    SizeLimits size_limits;
    /// How the materialized right input is kept as it grows: compressed, then spilled.
    BlockNestedLoopStoreSettings store_settings;
    /// Limits on a result block, from `max_block_size` / `max_joined_block_size_rows` and
    /// `max_joined_block_size_bytes`.
    size_t max_block_size;
    size_t max_block_bytes;
};

}
