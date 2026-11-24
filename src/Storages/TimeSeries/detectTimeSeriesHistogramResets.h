#pragma once

#include <base/types.h>

#include <span>


namespace DB
{
class ColumnTuple;

/// Sets the counter reset hints of a uniquely owned tuple column of histogram samples sorted and deduplicated by timestamp within each
/// of the ranges `offsets` delimit, by `TimeSeriesHistogramCounterResetRule::detectReset`: every sample but the first of a range against
/// its predecessor, the first one with `NO_PREVIOUS`.
void detectTimeSeriesHistogramResets(ColumnTuple & histograms, std::span<const size_t> offsets);

/// Sets the hint of the first sample of each of the ranges `offsets` delimit in a uniquely owned tuple column of histogram samples by
/// `TimeSeriesHistogramCounterResetRule::detectReset` with `NO_PREVIOUS`, keeping the hints of the other samples.
void setFirstTimeSeriesHistogramHints(ColumnTuple & histograms, std::span<const size_t> offsets);

}
