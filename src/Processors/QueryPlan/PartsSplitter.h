#pragma once

#include <functional>

#include <Interpreters/Context_fwd.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/KeyDescription.h>
#include <Storages/MergeTree/RangesInDataPart.h>


namespace DB
{

using ReadingInOrderStepGetter = std::function<Pipe(RangesInDataParts)>;

/// Splits parts into layers, each layer will contain parts subranges with PK values from its own range.
/// A separate pipe will be constructed for each layer with a reading step (provided by the reading_step_getter) and a filter for this layer's range of PK values.
/// Will try to produce exactly max_layer pipes but may return less if data is distributed in not a very parallelizable way.
Pipes buildPipesForReadingByPKRanges(
    const KeyDescription & primary_key,
    RangesInDataParts parts,
    size_t max_layers,
    ContextPtr context,
    ReadingInOrderStepGetter && reading_step_getter);
}
