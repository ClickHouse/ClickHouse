#pragma once

#include <Processors/QueryPlan/QueryPlan.h>

#include <memory>

namespace DB
{

class ReadFromMerge;
class SortingStep;
struct KeyDescription;
struct InputOrderInfo;
using InputOrderInfoPtr = std::shared_ptr<const InputOrderInfo>;

namespace QueryPlanOptimizations
{

/// Returns the node of the reading step that `optimizeReadInOrder` would find below a sorting step placed on top of `node`,
/// descending only through the steps it descends itself (expressions, filters, preliminary `DISTINCT`, set-building steps,
/// and joins with `read_in_order_through_join`), or `nullptr` if there is no such read or it is already read in order.
QueryPlan::Node * findReadingStepForReadInOrder(QueryPlan::Node & node, bool read_in_order_through_join);

/// Returns the input order that `optimizeReadInOrder` would request to satisfy the query's `sorting` step by reading rows
/// in `sorting_key` order, or `nullptr` if reading in order would not be useful. Its `direction` is the reading direction,
/// which is the direction of the sort description flipped by the reverse flags of the sorting key.
InputOrderInfoPtr getInputOrderIfReadInOrderIsUseful(
    const SortingStep & sorting,
    const KeyDescription & sorting_key,
    const QueryPlan::Node & subtree_above_reading);

/// The same for a `Merge` table: the input order that `optimizeReadInOrder` would request from every selected child table,
/// or `nullptr` if reading in order would not be useful for some child or the children would be read in different orders.
/// Creates the child plans of `merge`.
InputOrderInfoPtr getInputOrderIfReadInOrderIsUseful(
    const SortingStep & sorting,
    ReadFromMerge & merge,
    const QueryPlan::Node & subtree_above_reading);

}

}
