#pragma once

#include <Planner/PlannerContext.h>

#include <Core/SortDescription.h>

#include <vector>

namespace DB
{

class PlannerQueryProcessingInfo;
class ConstantNode;
class IQueryPlanStep;

struct QueryAnalysisResult
{
    bool aggregate_overflow_row = false;
    bool aggregate_final = false;
    bool aggregation_with_rollup_or_cube_or_grouping_sets = false;
    bool aggregation_should_produce_results_in_order_of_bucket_number = false;
    bool query_has_array_join_in_join_tree = false;
    bool query_has_with_totals_in_any_subquery_in_join_tree = false;
    SortDescription sort_description;

    /// Scalar subqueries whose value became the LIMIT or OFFSET, as in `LIMIT (SELECT ...)`. They
    /// cannot be linked the way every other consumer is: the walk that attributes a subquery to the
    /// step that reads it looks inside `ActionsDAG`s, and `LimitStep` keeps its bound as a plain
    /// field rather than an expression. So the ids are carried here and put on the step directly.
    std::vector<size_t> limit_offset_subquery_ids;

    /// Collects the ids of the scalar subqueries folded into a LIMIT or OFFSET constant.
    void collectLimitOffsetSubqueryIdsFrom(const ConstantNode & constant);

    /// Marks `step` as reading the scalar subqueries of the LIMIT and OFFSET.
    void addLimitOffsetSubqueryIdsTo(IQueryPlanStep & step) const;

    UInt64 limit_length = 0;
    UInt64 limit_offset = 0;
    Float64 fractional_limit = 0;
    Float64 fractional_offset = 0;
    UInt64 partial_sorting_limit = 0;
    bool is_limit_length_negative = false;
    bool is_limit_offset_negative = false;

    UInt64 limit_by_length = 0;
    UInt64 limit_by_offset = 0;
    bool is_limit_by_length_negative = false;
    bool is_limit_by_offset_negative = false;

public:
    static QueryAnalysisResult create(const QueryTreeNodePtr & query_tree,
        const PlannerQueryProcessingInfo & query_processing_info,
        const PlannerContextPtr & planner_context);
};

}
