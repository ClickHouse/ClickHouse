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

namespace
{
    /// Comparing an unsigned value with a constant zero directly is rejected by the compiler as a tautology.
    bool isBefore(UInt64 version, UInt64 other_version)
    {
        return version < other_version;
    }

    bool isAfter(UInt64 version, UInt64 other_version)
    {
        return version > other_version;
    }
}

bool isTimeSeriesVersionSupported(UInt64 version)
{
    return !isBefore(version, TimeSeriesVersion::MIN_SUPPORTED) && !isAfter(version, TimeSeriesVersion::LATEST);
}

void checkTimeSeriesVersionIsSupported(const StorageTimeSeries & time_series_storage)
{
    UInt64 version = time_series_storage.getVersion();

    if (isBefore(version, TimeSeriesVersion::MIN_SUPPORTED))
    {
        throw Exception(ErrorCodes::INCOMPATIBLE_SCHEMA,
            "{}: The TimeSeries table has version {} which is too old for this server "
            "(the minimum supported version is {}). The table can be dropped, "
            "or migrated using a version of ClickHouse which still supports it",
            time_series_storage.getStorageID().getNameForLogs(), version, TimeSeriesVersion::MIN_SUPPORTED);
    }

    if (isAfter(version, TimeSeriesVersion::LATEST))
    {
        throw Exception(ErrorCodes::INCOMPATIBLE_SCHEMA,
            "{}: The TimeSeries table has version {} which is newer than the latest version {} known to this server. "
            "Please upgrade ClickHouse to use this table",
            time_series_storage.getStorageID().getNameForLogs(), version, TimeSeriesVersion::LATEST);
    }
}

void checkTimeSeriesVersionIsWritable(const StorageTimeSeries & time_series_storage)
{
    checkTimeSeriesVersionIsSupported(time_series_storage);

    UInt64 version = time_series_storage.getVersion();

    if (isBefore(version, TimeSeriesVersion::MIN_WRITABLE))
    {
        throw Exception(ErrorCodes::INCOMPATIBLE_SCHEMA,
            "{}: The TimeSeries table has version {} which is too old to write into on this server "
            "(the minimum writable version is {}). Please re-create the table: create a new TimeSeries table, "
            "copy the data with an INSERT-SELECT query from the old table into the new one, "
            "and then replace the old table with the new one",
            time_series_storage.getStorageID().getNameForLogs(), version, TimeSeriesVersion::MIN_WRITABLE);
    }
}

void checkTimeSeriesVersionSupportedByPromQL(const StorageTimeSeries & time_series_storage)
{
    checkTimeSeriesVersionIsSupported(time_series_storage);

    UInt64 version = time_series_storage.getVersion();

    if (isBefore(version, TimeSeriesVersion::MIN_SUPPORTED_BY_PROMQL))
    {
        throw Exception(ErrorCodes::INCOMPATIBLE_SCHEMA,
            "{}: The TimeSeries table has version {} which is too old for the PromQL engine of this server "
            "(the minimum version supported by PromQL is {}). Please re-create the table: create a new TimeSeries table, "
            "copy the data with an INSERT-SELECT query from the old table into the new one, "
            "and then replace the old table with the new one",
            time_series_storage.getStorageID().getNameForLogs(), version, TimeSeriesVersion::MIN_SUPPORTED_BY_PROMQL);
    }
}

}
