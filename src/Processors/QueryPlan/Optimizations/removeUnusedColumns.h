#pragma once

#include <Processors/QueryPlan/QueryPlan.h>

#include <vector>

namespace DB::QueryPlanOptimizations
{

/// When the parent step removed some inputs but the child step couldn't fully reduce its output
/// to match (e.g., ReadFromMergeTree with FINAL must keep columns required for merging),
/// adjust the parent step to accept the extra columns from the child by adding them as
/// consumed DAG inputs and setting the input header to match the child's output exactly.
/// Also sets the `prevent_input_removal` flag to ensure these absorbed columns are not
/// removed on subsequent optimization passes.
/// Works with both ExpressionStep and FilterStep parents.
///
/// Uses required_positions (what the parent asked for) and kept_output_positions
/// (what the child actually kept) to identify extra columns by position,
/// which is correct even with duplicate column names.
bool absorbExtraChildColumns(
    QueryPlan::Node & node,
    size_t child_id,
    const std::vector<size_t> & required_positions,
    const std::vector<size_t> & kept_output_positions);

/// Resolves the `kept_output_positions` field of `RemoveUnusedColumnsResult` into an
/// explicit position list. When a step is unchanged (`changed` is false), the result's
/// `kept_output_positions` is empty by convention (meaning all original positions are
/// kept). This helper moves the vector through when it is already populated, or builds
/// the identity list [0, 1, ..., N-1] otherwise.
std::vector<size_t> effectiveKeptOutputPositions(
    bool changed,
    std::vector<size_t> && kept_output_positions,
    size_t num_output_columns);

}
