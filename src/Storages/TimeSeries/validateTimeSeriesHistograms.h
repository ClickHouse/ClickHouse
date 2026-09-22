#pragma once

#include <Core/Block.h>


namespace DB
{

/// Validates histogram samples before they are written to the "histograms" target table (`histograms_block` has the shape
/// of that table). `max_buckets` is the value of the `histograms_max_buckets` setting, 0 means no limit.
/// Throws INCORRECT_DATA describing the first invalid histogram.
void validateTimeSeriesHistograms(const Block & histograms_block, UInt64 max_buckets);

}
