#pragma once

#include <DataTypes/IDataType.h>


namespace DB
{

struct TimeSeriesSettings;

/// The kinds of samples of a time series, a bitmask stored in the column `sample_kinds` of the "time ranges" table:
/// every row has the kinds of the samples written together with it, so OR over the rows of a time series whose
/// time ranges intersect a time range covers all of its samples in it. `timeSeriesSelector` reads only the target tables of
/// the kinds present. Tables with histograms (`TimeSeriesVersion::MIN_WITH_HISTOGRAMS_TARGET`) storing the time range
/// (`store_time_ranges`) have the column, like `min_time` and `max_time`. It has no default: the TimeSeries
/// table writes it with every row, and whoever writes the "time ranges" table another way is responsible for the right kinds.
/// The engine of the "time ranges" table must keep the kinds when it merges rows: the generated one does, a customized inner one
/// is checked on creation.
class TimeSeriesSampleKinds
{
public:
    /// Values are important.
    static constexpr UInt8 None = 0;
    static constexpr UInt8 Float = 1;
    static constexpr UInt8 Histogram = 1 << 1;
    static constexpr UInt8 All = Float | Histogram;

    static bool isStored(const TimeSeriesSettings & settings);

    static const DataTypePtr & getDataType(bool aggregated);

    static bool isValidDataType(const IDataType & type, bool & aggregated);
};

}
