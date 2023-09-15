#include "ReplicatedFetchesLog.h"
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes//DataTypeDate.h>
#include <DataTypes//DataTypeDateTime.h>
#include <DataTypes//DataTypeDateTime64.h>
#include <Interpreters/Context.h>
#include <Storages/MergeTree/ReplicatedFetchList.h>

namespace DB
{

NamesAndTypesList ReplicatedFetchLogElement::getNamesAndTypes()
{
    return
    {
        {"event_date", std::make_shared<DataTypeDate>()},
        {"event_time", std::make_shared<DataTypeDateTime>()},
        {"event_time_microseconds", std::make_shared<DataTypeDateTime64>(6)},
        {"event_start_time_microseconds", std::make_shared<DataTypeDateTime64>(6)},
        {"database", std::make_shared<DataTypeString>()},
        {"table", std::make_shared<DataTypeString>()},
        {"elapsed", std::make_shared<DataTypeFloat64>()},
        {"progress", std::make_shared<DataTypeFloat64>()},
        {"result_part_name", std::make_shared<DataTypeString>()},
        {"result_part_path", std::make_shared<DataTypeString>()},
        {"partition_id", std::make_shared<DataTypeString>()},
        {"total_size_bytes_compressed", std::make_shared<DataTypeUInt64>()},
        {"bytes_read_compressed", std::make_shared<DataTypeUInt64>()},
        {"source_replica_path", std::make_shared<DataTypeString>()},
        {"source_replica_hostname", std::make_shared<DataTypeString>()},
        {"source_replica_port", std::make_shared<DataTypeUInt16>()},
        {"interserver_scheme", std::make_shared<DataTypeString>()},
        {"URI", std::make_shared<DataTypeString>()},
        {"to_detached", std::make_shared<DataTypeUInt8>()},
        {"thread_id", std::make_shared<DataTypeUInt64>()},
        {"exception_code", std::make_shared<DataTypeUInt64>()},
        {"exception", std::make_shared<DataTypeString>()}
    };
}

void ReplicatedFetchLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t i = 0;
    columns[i++]->insert(DateLUT::instance().toDayNum(event_time).toUnderType());
    columns[i++]->insert(event_time);
    columns[i++]->insert(event_time_microseconds);
    columns[i++]->insert(event_time_microseconds - UInt64(info.elapsed * 1e6));
    columns[i++]->insert(info.database);
    columns[i++]->insert(info.table);
    columns[i++]->insert(info.elapsed);
    columns[i++]->insert(info.progress);
    columns[i++]->insert(info.result_part_name);
    columns[i++]->insert(info.result_part_path);
    columns[i++]->insert(info.partition_id);
    columns[i++]->insert(info.total_size_bytes_compressed);
    columns[i++]->insert(info.bytes_read_compressed);
    columns[i++]->insert(info.source_replica_path);
    columns[i++]->insert(info.source_replica_hostname);
    columns[i++]->insert(info.source_replica_port);
    columns[i++]->insert(info.interserver_scheme);
    columns[i++]->insert(info.uri);
    columns[i++]->insert(info.to_detached);
    columns[i++]->insert(info.thread_id);
    columns[i++]->insert(exception_code);
    columns[i++]->insert(exception);
}

bool ReplicatedFetchesLog::addNewEntry(const ContextPtr & current_context, const ReplicatedFetchInfo & info, const ExecutionStatus & es)
{
    auto replicated_fetches_log = current_context->getReplicatedFetchesLog();
    if (!replicated_fetches_log)
        return false;
    ReplicatedFetchLogElement elem;
    const auto time_now = std::chrono::system_clock::now();
    elem.event_time = timeInSeconds(time_now);
    elem.event_time_microseconds = timeInMicroseconds(time_now);
    elem.info = info;
    elem.exception_code = es.code;
    elem.exception = es.message;
    replicated_fetches_log->add(elem);
    return true;
}
}
