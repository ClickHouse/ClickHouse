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

/// How many leading columns of `sorting`'s sort description reading rows in `sorting_key` order supplies.
/// When this equals the whole description, the read leaves no sorting work; a shorter prefix means the
/// rows still have to be sorted within each prefix group.
size_t readInOrderSortedPrefixLength(
    const SortingStep & sorting,
    const KeyDescription & sorting_key,
    const QueryPlan::Node & subtree_above_reading);

}

}
