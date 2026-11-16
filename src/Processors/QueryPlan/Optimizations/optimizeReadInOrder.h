#pragma once

#include <Processors/QueryPlan/QueryPlan.h>

namespace DB
{

class SortingStep;
struct KeyDescription;

namespace QueryPlanOptimizations
{

/// Returns true if reading rows in `sorting_key` order would let `optimizeReadInOrder` satisfy the query's `sorting` step.
bool wouldReadInOrderBeUseful(
    const SortingStep & sorting,
    const KeyDescription & sorting_key,
    const QueryPlan::Node & subtree_above_reading);

}

}
