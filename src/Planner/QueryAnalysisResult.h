#pragma once

#include <Planner/PlannerContext.h>

#include <Core/SortDescription.h>

namespace DB
{

class PlannerQueryProcessingInfo;

struct QueryAnalysisResult
{
    bool aggregate_overflow_row = false;
    bool aggregate_final = false;
    bool aggregation_with_rollup_or_cube_or_grouping_sets = false;
    bool aggregation_should_produce_results_in_order_of_bucket_number = false;
    bool query_has_array_join_in_join_tree = false;
    bool query_has_with_totals_in_any_subquery_in_join_tree = false;
    SortDescription sort_description;
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
