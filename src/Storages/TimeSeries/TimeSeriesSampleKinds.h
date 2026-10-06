#pragma once

#include <DataTypes/IDataType.h>


namespace DB
{

struct TimeSeriesSettings;

/// The kinds of samples of a time series, a bitmask stored in the column `sample_kinds` of the "tags" table next to `min_time`
/// and `max_time`: every row has the kinds of the samples written together with it, so OR over the rows of a time series whose
/// time ranges intersect a time range covers all of its samples in it. `timeSeriesSelector` reads only the target tables of
/// the kinds present. Tables with histograms (`TimeSeriesVersion::MIN_WITH_HISTOGRAMS_TARGET`) storing the time range
/// (`store_min_time_and_max_time`) must have the column, like `min_time` and `max_time`. It has no default: the TimeSeries
/// table writes it with every row, and whoever writes the "tags" table another way is responsible for the right kinds.
/// The engine of the "tags" table must keep the kinds when it merges rows: the generated one does, a customized inner one
/// is checked on creation, an external one is the responsibility of its owner.
class TimeSeriesSampleKinds
{
public:
    static constexpr UInt8 Floats = 1;
    static constexpr UInt8 Histograms = 1 << 1;
    static constexpr UInt8 All = Floats | Histograms;

    static bool isStored(const TimeSeriesSettings & settings);

    static const DataTypePtr & getDataType(bool aggregated);

    static bool isValidDataType(const IDataType & type, bool & aggregated);
};

}
