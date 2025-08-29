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
#include <Common/SipHash.h>
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
                parseQuery(codec_parser, "(ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
                "Hostname of the server."});

    result.add({"event_date",
                std::make_shared<DataTypeDate>(),
                parseQuery(codec_parser, "(Delta(2), ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
                "Date the group was flushed."});

    result.add({"event_time",
                std::make_shared<DataTypeDateTime>(),
                parseQuery(codec_parser, "(Delta(4), ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
                "Time the group was flushed."});

    result.add({"session_id",
                std::make_shared<DataTypeInt64>(),
                parseQuery(codec_parser, "(ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
                "Session id."});

    result.add({"parent_path",
                std::make_shared<DataTypeString>(),
                parseQuery(codec_parser, "(ZSTD(3))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
                "Prefix of the path."});

    result.add({"operation",
                Coordination::SystemTablesDataTypes::operationEnum(),
                parseQuery(codec_parser, "(ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
                "Type of ZooKeeper operation."});

    result.add({"count",
                std::make_shared<DataTypeUInt32>(),
                parseQuery(codec_parser, "(ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
                "Number of operations in the (session_id, parent_path, operation) group."});

    result.add({"errors",
                std::make_shared<DataTypeMap>(Coordination::SystemTablesDataTypes::errorCodeEnum(), std::make_shared<DataTypeUInt32>()),
                parseQuery(codec_parser, "(ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
                "Errors in the (session_id, parent_path, operation) group."});

    result.add({"average_latency",
                std::make_shared<DataTypeFloat64>(),
                parseQuery(codec_parser, "(ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
                "Average latency across all operations in (session_id, parent_path, operation) group, in microseconds."});
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
    columns[i++]->insert(count);
    errors->dumpToMapColumn(&typeid_cast<DB::ColumnMap &>(*columns[i++]));
    columns[i++]->insert(static_cast<Float64>(total_latency_microseconds) / count);
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
            .count = entry_stats.count,
            .errors = std::move(entry_stats.errors),
            .total_latency_microseconds = entry_stats.total_latency_microseconds,
        };
        add(std::move(element));
    }
}

void AggregatedZooKeeperLog::observe(Int64 session_id, Coordination::OpNum operation, const std::filesystem::path & path, UInt64 latency_microseconds, Coordination::Error error)
{
    std::lock_guard lock(stats_mutex);
    stats[EntryKey{.session_id = session_id, .operation = operation, .parent_path = path.parent_path()}].observe(latency_microseconds, error);
}

bool AggregatedZooKeeperLog::EntryKey::operator==(const EntryKey & other) const
{
    return session_id == other.session_id && operation == other.operation && parent_path == other.parent_path;
}

size_t AggregatedZooKeeperLog::EntryKeyHash::operator()(const EntryKey & entry_key) const
{
    SipHash hash;
    hash.update(entry_key.session_id);
    hash.update(entry_key.operation);
    hash.update(entry_key.parent_path);
    return hash.get64();
}

void AggregatedZooKeeperLog::EntryStats::observe(UInt64 latency_microseconds, Coordination::Error error)
{
    ++count;
    total_latency_microseconds += latency_microseconds;
    errors->increment(error);
}

}
