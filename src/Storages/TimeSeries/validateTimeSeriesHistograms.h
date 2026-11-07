#pragma once

#include <Core/Block.h>


namespace DB
{

/// Validates histogram samples before they are written to the samples table. `histograms_block` has the outer `histograms.*`
/// columns (see TimeSeriesHistogramsColumns), `histograms.timestamp` included: arrays sharing one offsets column, whose concatenated
/// elements are the samples. The rules are Prometheus's `Histogram.Validate` plus the shape rules of the storage: only the flavour
/// bit of `flags` is set, the bucket indexes increase strictly, the counts of an integer histogram add up to `count_int`. The
/// elements of the other flavour are not checked, the readers ignore them. A bucket with a count of 0 passes, as in Prometheus;
/// a gap between two bucket indexes which no `Int32` span offset can express doesn't. `max_buckets` is the value of the
/// `histograms_max_buckets` setting, 0 means no limit. Throws INCORRECT_DATA describing the first invalid histogram: its number
/// among the samples of the block, the row of the block and its timestamp.
void validateTimeSeriesHistograms(const Block & histograms_block, UInt64 max_buckets);

}
