#pragma once

#include <functional>

#include <Interpreters/Context_fwd.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/KeyDescription.h>
#include <Storages/MergeTree/RangesInDataPart.h>


namespace DB
{

using ReadingInOrderStepGetter = std::function<Pipe(RangesInDataParts)>;

struct SplitPartsWithRangesByPrimaryKeyResult
{
    RangesInDataParts non_intersecting_parts_ranges;
    Pipes merging_pipes;
};

/** Splits parts ranges into:
  *
  * 1. Non interesecing part ranges, for parts with level > 0.
  * 2. Merging layers, that contain ranges from multiple parts. A separate pipe will be constructed for each layer
  * with a reading step (provided by the in_order_reading_step_getter) and a filter for this layer's range of PK values.
  *
  * Will try to produce exactly max_layer layers but may return less if data is distributed in not a very parallelizable way.
  */
SplitPartsWithRangesByPrimaryKeyResult splitPartsWithRangesByPrimaryKey(
    const KeyDescription & primary_key,
    ExpressionActionsPtr sorting_expr,
    RangesInDataParts parts,
    size_t max_layers,
    ContextPtr context,
    ReadingInOrderStepGetter && in_order_reading_step_getter,
    bool split_parts_ranges_into_intersecting_and_non_intersecting,
    bool split_intersecting_parts_ranges_into_layers);
}
