#pragma once

#include <base/types.h>

#include <span>


namespace DB
{
class ColumnTuple;

/// Sets the counter reset hints of a uniquely owned tuple column of histogram samples sorted and deduplicated by timestamp, as the sink
/// stores them: `series_offsets` delimit the series (the input rows), independently of the output buckets. Within a series every
/// sample but the first gets the hint of `TimeSeriesHistogramCounterResetRule::hintAfterPredecessor` against its predecessor; the first
/// sample keeps the hint it was sent with, unless it is a stale marker or `duplicate_hints_differ` is set for it: then its hint is
/// unknown. Sample `i` of the column is sample `sorted_indices[i]` of the input, which indexes `duplicate_hints_differ`.
void detectTimeSeriesHistogramResets(
    ColumnTuple & histograms,
    std::span<const size_t> series_offsets,
    std::span<const size_t> sorted_indices,
    std::span<const UInt8> duplicate_hints_differ);

}
