#include <Interpreters/AggregatedZooKeeperLog.h>

#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/parseQuery.h>
#include <Storages/ColumnsDescription.h>
#include <Columns/ColumnMap.h>
#include <base/getFQDNOrHostName.h>
#include <city.h>
#include <Common/DateLUTImpl.h>
#include <Common/ZooKeeper/SystemTablesDataTypes.h>


namespace DB
{

ColumnsDescription AggregatedZooKeeperLogElement::getColumnsDescription()
{
    ColumnsDescription result;
    ParserCodec codec_parser;

    result.add({"hostname",
                std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()),
                "Hostname of the server."});

    result.add({"event_date",
                std::make_shared<DataTypeDate>(),
                "Date the group was flushed."});

    result.add({"event_time",
                std::make_shared<DataTypeDateTime>(),
                "Time the group was flushed."});

    result.add({"session_id",
                std::make_shared<DataTypeInt64>(),
                "Session id."});

    result.add({"parent_path",
                std::make_shared<DataTypeString>(),
                "Prefix of the path."});

    result.add({"operation",
                Coordination::SystemTablesDataTypes::operationEnum(),
                "Type of ZooKeeper operation."});

    result.add({"is_subrequest",
                std::make_shared<DataTypeUInt8>(),
                "Whether this operation was a subrequest inside a Multi or MultiRead operation."});

    result.add({"count",
                std::make_shared<DataTypeUInt32>(),
                "Number of operations in the (session_id, parent_path, operation, component, is_subrequest) group."});

    result.add({"errors",
                std::make_shared<DataTypeMap>(Coordination::SystemTablesDataTypes::errorCodeEnum(), std::make_shared<DataTypeUInt32>()),
                "Errors in the (session_id, parent_path, operation, component, is_subrequest) group."});

    result.add({"average_latency",
                std::make_shared<DataTypeFloat64>(),
                "Average latency across all operations in (session_id, parent_path, operation, component, is_subrequest) group, in microseconds. Subrequests have zero latency because the latency is attributed to the enclosing Multi or MultiRead operation."});

    result.add({"component",
                std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()),
                "Component that caused the event."});
    return result;
}

void AggregatedZooKeeperLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t i = 0;
    columns[i++]->insert(getFQDNOrHostName());
    columns[i++]->insert(DateLUT::instance().toDayNum(event_time).toUnderType());
    columns[i++]->insert(event_time);
    columns[i++]->insert(session_id);
    columns[i++]->insert(parent_path);
    columns[i++]->insert(operation);
    columns[i++]->insert(static_cast<UInt8>(is_subrequest));
    columns[i++]->insert(count);
    errors->dumpToMapColumn(&typeid_cast<DB::ColumnMap &>(*columns[i++]));
    columns[i++]->insert(static_cast<Float64>(total_latency_microseconds) / count);
    columns[i++]->insert(component.view());
}

void AggregatedZooKeeperLog::stepFunction(TimePoint current_time)
{
    std::unordered_map<EntryKey, EntryStats, EntryKeyHash> local_stats;
    {
        std::lock_guard lock(stats_mutex);
        std::swap(stats, local_stats);
    }

    for (auto & [entry_key, entry_stats] : local_stats)
    {
        AggregatedZooKeeperLogElement element{
            .event_time = std::chrono::system_clock::to_time_t(current_time),
            .session_id = entry_key.session_id,
            .parent_path = entry_key.parent_path,
            .operation = entry_key.operation,
            .component = entry_key.component,
            .is_subrequest = entry_key.is_subrequest,
            .count = entry_stats.count,
            .errors = std::move(entry_stats.errors),
            .total_latency_microseconds = entry_stats.total_latency_microseconds,
        };
        add(std::move(element));
    }
}

void AggregatedZooKeeperLog::observe(
    Int64 session_id,
    Int32 operation,
    const std::filesystem::path & path,
    UInt64 latency_microseconds,
    Coordination::Error error,
    StaticString component,
    bool is_subrequest)
{
    std::lock_guard lock(stats_mutex);

    EntryKey entry_key{
        .session_id = session_id,
        .operation = operation,
        .parent_path = path.parent_path(),
        .component = component,
        .is_subrequest = is_subrequest
    };
    stats[std::move(entry_key)].observe(latency_microseconds, error);
}

size_t AggregatedZooKeeperLog::EntryKeyHash::operator()(const EntryKey & entry_key) const
{
    return CityHash_v1_0_2::CityHash64WithSeed(
        entry_key.parent_path.data(),
        entry_key.parent_path.size(),
        static_cast<uint64_t>(entry_key.operation) ^ static_cast<uint64_t>(entry_key.session_id) ^ entry_key.is_subrequest);
}

void AggregatedZooKeeperLog::EntryStats::observe(UInt64 latency_microseconds, Coordination::Error error)
{
    ++count;
    total_latency_microseconds += latency_microseconds;
    errors->increment(error);
}

}
