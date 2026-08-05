#pragma once

#include <Processors/QueryPlan/QueryPlan.h>

namespace DB
{

class SortingStep;
struct KeyDescription;
struct InputOrderInfo;
using InputOrderInfoPtr = std::shared_ptr<const InputOrderInfo>;

namespace QueryPlanOptimizations
{

/// Returns the input order that `optimizeReadInOrder` would request to satisfy the query's `sorting` step by reading rows
/// in `sorting_key` order, or `nullptr` if reading in order would not be useful. Its `direction` is the reading direction
/// after applying the sorting key's per-column reverse flags.
InputOrderInfoPtr getInputOrderIfReadInOrderIsUseful(
    const SortingStep & sorting,
    const KeyDescription & sorting_key,
    const QueryPlan::Node & subtree_above_reading);

}

}
