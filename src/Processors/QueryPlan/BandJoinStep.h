#pragma once

#include <Core/Joins.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/Transforms/BandJoinTransform.h>

namespace DB
{

/// Joins two data streams by the band shape `point {>,>=} lo AND point {<,<=} hi`, where the
/// point expression comes from one table and both bounds from the other. Only the interval
/// side is materialized (pre-sorted by `lo` at plan level); the point side streams and probes
/// a shared read-only index in parallel. The point side may be either query input: when it is
/// the right one, the step swaps the input pipelines so the point side probes and restores the
/// query column order on top of the join (`Swapped: true` in EXPLAIN).
class BandJoinStep : public IQueryPlanStep
{
public:
    BandJoinStep(
        const SharedHeader & left_header_,
        const SharedHeader & right_header_,
        BandJoinConditions conditions_,
        JoinKind kind_,
        JoinStrictness strictness_,
        bool point_side_is_right_,
        const SizeLimits & size_limits_,
        size_t max_joined_block_rows_,
        size_t max_joined_block_bytes_);

    /// Whether the step can execute this join type with the point expression on that side.
    static bool isSupportedJoinType(JoinKind kind, JoinStrictness strictness, bool point_side_is_right);

    String getName() const override { return "BandJoin"; }

    QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings &) override;

    void describePipeline(FormatSettings & settings) const override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

private:
    void updateOutputHeader() override;

    String formatConditions() const;

    /// The two bounds with the positions resolved against the point-side and interval-side
    /// headers; [0] is the lower bound, [1] the upper.
    BandJoinConditions conditions;

    BandJoinKind kind = BandJoinKind::Inner;
    /// Whether to swap the input pipelines so the point side probes; set when the point
    /// expression comes from the query's right table.
    bool swap_inputs = false;

    /// Limits on the materialized interval side, from `max_rows_in_join` / `max_bytes_in_join`.
    SizeLimits size_limits;
    /// Output chunk caps, from `max_joined_block_size_rows` / `max_joined_block_size_bytes`.
    size_t max_joined_block_rows;
    size_t max_joined_block_bytes;
};

}
