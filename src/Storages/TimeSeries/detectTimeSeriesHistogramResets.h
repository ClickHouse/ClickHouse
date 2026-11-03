#pragma once

#include <cstddef>
#include <span>


namespace DB
{
class ColumnTuple;

/// Updates hints in a uniquely owned tuple column of sorted, deduplicated histograms. `original_indices` maps the samples to
/// their input positions; `series_offsets` delimit the input series, independently of the output buckets.
/// Stale markers always have an unknown hint. Otherwise, the first hint of each series and gauge markers are preserved.
/// An explicit reset is trusted only if sorting and deduplication retained its original predecessor.
void detectTimeSeriesHistogramResets(
    ColumnTuple & histograms,
    std::span<const size_t> original_indices,
    std::span<const size_t> series_offsets);

}
