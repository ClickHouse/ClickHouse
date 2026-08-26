#pragma once

#include <Core/Joins.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/Transforms/IEJoinTransform.h>

namespace DB
{

/// Joins two data streams by two inequality conditions with the IEJoin algorithm.
/// The step speaks the query terms: conditions and join type refer to the query's left and
/// right tables. Right-side SEMI/ANTI are executed as the left-side mirror internally: the
/// step swaps the input pipelines, reverses the operators, and restores the column order.
/// An optional residual condition (the ON conjuncts beyond the two inequalities, a single
/// boolean expression over columns of both inputs) gates candidate pairs inside the operator.
class IEJoinStep : public IQueryPlanStep
{
public:
    IEJoinStep(
        const SharedHeader & left_header_,
        const SharedHeader & right_header_,
        IEJoinConditions conditions_,
        ExpressionActionsPtr residual_condition_,
        JoinKind kind_,
        JoinStrictness strictness_,
        bool inputs_sorted_by_first_key_,
        const SizeLimits & size_limits_,
        size_t max_block_size_,
        size_t max_block_bytes_);

    /// Whether the step can execute this join type.
    static bool isSupportedJoinType(JoinKind kind, JoinStrictness strictness);

    String getName() const override { return "IEJoin"; }

    QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings &) override;

    void describePipeline(FormatSettings & settings) const override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

private:
    void updateOutputHeader() override;

    String formatConditions() const;

    /// Conditions in the query orientation (`left` refers to the query's left table).
    IEJoinConditions conditions;
    /// The residual ON condition with its inputs resolved against the query-orientation
    /// headers, if any.
    std::optional<IEJoinResidualCondition> residual;

    /// The executed join type and whether to swap the input pipelines for it,
    /// derived from the query kind/strictness.
    IEJoinKind kind = IEJoinKind::Inner;
    bool swap_inputs = false;

    /// The planner pre-sorted each input by its first-condition key with a `SortingStep`
    /// (always ascending, NULLS LAST); selects the merge-based L1 build in the operator.
    bool inputs_sorted_by_first_key;
    /// Limits on the materialized input, from `max_rows_in_join` / `max_bytes_in_join`.
    SizeLimits size_limits;
    /// Limits on a result block, from `max_block_size` / `max_joined_block_size_rows` and
    /// `max_joined_block_size_bytes`.
    size_t max_block_size;
    size_t max_block_bytes;
};

}
