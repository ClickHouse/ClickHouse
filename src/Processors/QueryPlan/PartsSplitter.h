#pragma once

#include <functional>

#include <Interpreters/Context_fwd.h>
#include <Storages/KeyDescription.h>
#include <Storages/MergeTree/RangesInDataPart.h>


namespace DB
{

using ReadingStepGetter = std::function<Pipe(RangesInDataParts)>;

Pipes buildPipesForReading(
    const KeyDescription & primary_key,
    RangesInDataParts parts,
    size_t max_layers,
    ContextPtr context,
    ReadingStepGetter && reading_step_getter);
}
