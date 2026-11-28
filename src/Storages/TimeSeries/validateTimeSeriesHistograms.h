#pragma once

#include <DataTypes/IDataType_fwd.h>
#include <base/types.h>


namespace DB
{
class ColumnArray;

/// Validates histogram samples before they are written to the samples table. `histograms` is the outer `histograms` column of an
/// INSERT (see TimeSeriesHistogramsColumns), whose concatenated arrays are the samples, and `timestamp_type` the type of their
/// timestamps. The rules are Prometheus's `Histogram.Validate` plus the shape rules of the storage: `is_float` is
/// 0 or 1, the bucket indexes increase strictly, the counts of an integer histogram add up to `count_int`. The
/// elements of the other flavour are not checked, the readers ignore them. A bucket with a count of 0 passes, as in Prometheus;
/// a gap between two bucket indexes which no `Int32` span offset can express doesn't. `max_buckets` is the value of the
/// `native_histogram_bucket_limit` setting, 0 means no limit. Throws INCORRECT_DATA describing the first invalid histogram: its number
/// among the samples of the block, the row of the block and its timestamp.
void validateTimeSeriesHistograms(const ColumnArray & histograms, const DataTypePtr & timestamp_type, UInt64 max_buckets);

}
