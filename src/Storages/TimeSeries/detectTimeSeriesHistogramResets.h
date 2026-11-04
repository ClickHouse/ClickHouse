#pragma once

#include <cstddef>
#include <span>


namespace DB
{
class ColumnTuple;

/// Sets the counter reset hints of a uniquely owned tuple column of histogram samples sorted and deduplicated by timestamp, as the sink
/// stores them: `series_offsets` delimit the series (the input rows), independently of the output buckets. Within a series every
/// sample but the first gets the hint of `TimeSeriesHistogramCounterResetRule::hintAfterPredecessor` against its predecessor; the first
/// sample keeps the hint it was sent with, except a stale marker, whose hint is unknown.
void detectTimeSeriesHistogramResets(ColumnTuple & histograms, std::span<const size_t> series_offsets);

}
