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

/// Returns true if `optimizeReadInOrder` would install a read-in-order plan for `sorting` over
/// `subtree_above_reading`, for any of the reading steps it supports (`ReadFromMergeTree`,
/// `ReadFromMerge`, `ReadFromObjectStorageStep`). The reading step is looked up with the same
/// traversal the optimization itself uses, so `read_in_order_through_join` and
/// `read_in_order_through_spilling_join` must be passed as the query has them.
/// This is a non-mutating probe: unlike `buildInputOrderInfo` it neither calls
/// `requestReadingInOrder` nor pins any join in order.
bool wouldReadInOrderBeUseful(
    const SortingStep & sorting,
    QueryPlan::Node & subtree_above_reading,
    bool read_in_order_through_join,
    bool read_in_order_through_spilling_join);

}

}
