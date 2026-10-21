#include <Storages/TimeSeries/TimeSeriesSampleKinds.h>

#include <DataTypes/DataTypeFactory.h>
#include <Parsers/Prometheus/TimeSeriesVersion.h>
#include <Storages/TimeSeries/TimeSeriesSettings.h>


namespace DB
{

namespace TimeSeriesSetting
{
    extern const TimeSeriesSettingsBool store_time_ranges;
    extern const TimeSeriesSettingsUInt64 version;
}

bool TimeSeriesSampleKinds::isStored(const TimeSeriesSettings & settings)
{
    return timeSeriesVersionSupportsHistograms(settings[TimeSeriesSetting::version])
        && settings[TimeSeriesSetting::store_time_ranges];
}

const DataTypePtr & TimeSeriesSampleKinds::getDataType(bool aggregated)
{
    static const DataTypePtr plain_type = DataTypeFactory::instance().get("UInt8");
    static const DataTypePtr aggregated_type = DataTypeFactory::instance().get("SimpleAggregateFunction(groupBitOr, UInt8)");
    return aggregated ? aggregated_type : plain_type;
}

bool TimeSeriesSampleKinds::isValidDataType(const IDataType & type, bool & aggregated)
{
    /// Compared by name: `equals` ignores the aggregate function of a `SimpleAggregateFunction` type.
    const auto type_name = type.getName();

    aggregated = type_name == getDataType(true)->getName();

    return aggregated || (type_name == getDataType(false)->getName());
}

}
