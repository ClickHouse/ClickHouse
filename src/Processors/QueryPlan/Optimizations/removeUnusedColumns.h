#pragma once

#include <Processors/QueryPlan/QueryPlan.h>

namespace DB::QueryPlanOptimizations
{

/// When the parent step removed some inputs but the child step couldn't fully reduce its output
/// to match (e.g., ReadFromMergeTree with FINAL must keep columns required for merging),
/// adjust the parent step to accept the extra columns from the child by adding them as
/// consumed DAG inputs and setting the input header to match the child's output exactly.
/// Also sets the `prevent_input_removal` flag to ensure these absorbed columns are not
/// removed on subsequent optimization passes.
/// Works with both ExpressionStep and FilterStep parents.
bool absorbExtraChildColumns(QueryPlan::Node & node, size_t child_id);

}
