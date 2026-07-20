#include <Storages/TimeSeries/TimeSeriesVersion.h>

#include <Common/Exception.h>
#include <Interpreters/StorageID.h>
#include <Storages/StorageTimeSeries.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int INCOMPATIBLE_SCHEMA;
}

void checkTimeSeriesVersionSupportedByPromQL(const StorageTimeSeries & time_series_storage)
{
    UInt64 version = time_series_storage.getVersion();

    if (version < TimeSeriesVersion::MIN_SUPPORTED_BY_PROMQL)
    {
        throw Exception(ErrorCodes::INCOMPATIBLE_SCHEMA,
            "{}: The TimeSeries table has version {} which is too old for the PromQL engine of this server "
            "(the minimum supported version is {}). Please re-create the table: create a new TimeSeries table, "
            "copy the data with an INSERT-SELECT query from the old table into the new one, "
            "and then replace the old table with the new one",
            time_series_storage.getStorageID().getNameForLogs(), version, TimeSeriesVersion::MIN_SUPPORTED_BY_PROMQL);
    }

    if (version > TimeSeriesVersion::LATEST)
    {
        throw Exception(ErrorCodes::INCOMPATIBLE_SCHEMA,
            "{}: The TimeSeries table has version {} which is newer than the latest version {} known to this server. "
            "Please upgrade ClickHouse to use this table",
            time_series_storage.getStorageID().getNameForLogs(), version, TimeSeriesVersion::LATEST);
    }
}

}
