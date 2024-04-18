#include <Storages/System/StorageSystemReplicatedFetches.h>
#include <Storages/MergeTree/ReplicatedFetchList.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Access/ContextAccess.h>

namespace DB
{

ColumnsDescription StorageSystemReplicatedFetches::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"database", std::make_shared<DataTypeString>(), "Name of the database."},
        {"table", std::make_shared<DataTypeString>(), "Name of the table."},
        {"elapsed", std::make_shared<DataTypeFloat64>(), "The time elapsed (in seconds) since showing currently running background fetches started."},
        {"progress", std::make_shared<DataTypeFloat64>(), "The percentage of completed work from 0 to 1."},
        {"result_part_name", std::make_shared<DataTypeString>(),
            "The name of the part that will be formed as the result of showing currently running background fetches."},
        {"result_part_path", std::make_shared<DataTypeString>(),
            "Absolute path to the part that will be formed as the result of showing currently running background fetches."},
        {"partition_id", std::make_shared<DataTypeString>(), "ID of the partition."},
        {"total_size_bytes_compressed", std::make_shared<DataTypeUInt64>(), "The total size (in bytes) of the compressed data in the result part."},
        {"bytes_read_compressed", std::make_shared<DataTypeUInt64>(), "The number of compressed bytes read from the result part."},
        {"source_replica_path", std::make_shared<DataTypeString>(), "Absolute path to the source replica."},
        {"source_replica_hostname", std::make_shared<DataTypeString>(), "Hostname of the source replica."},
        {"source_replica_port", std::make_shared<DataTypeUInt16>(), "Port number of the source replica."},
        {"interserver_scheme", std::make_shared<DataTypeString>(), "Name of the interserver scheme."},
        {"URI", std::make_shared<DataTypeString>(), "Uniform resource identifier."},
        {"to_detached", std::make_shared<DataTypeUInt8>(),
            "The flag indicates whether the currently running background fetch is being performed using the TO DETACHED expression."},
        {"thread_id", std::make_shared<DataTypeUInt64>(), "Thread identifier."},
    };
}

void StorageSystemReplicatedFetches::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    const auto access = context->getAccess();
    const bool check_access_for_tables = !access->isGranted(AccessType::SHOW_TABLES);

    for (const auto & fetch : context->getReplicatedFetchList().get())
    {
        if (check_access_for_tables && !access->isGranted(AccessType::SHOW_TABLES, fetch.database, fetch.table))
            continue;

        size_t i = 0;
        res_columns[i++]->insert(fetch.database);
        res_columns[i++]->insert(fetch.table);
        res_columns[i++]->insert(fetch.elapsed);
        res_columns[i++]->insert(fetch.progress);
        res_columns[i++]->insert(fetch.result_part_name);
        res_columns[i++]->insert(fetch.result_part_path);
        res_columns[i++]->insert(fetch.partition_id);
        res_columns[i++]->insert(fetch.total_size_bytes_compressed);
        res_columns[i++]->insert(fetch.bytes_read_compressed);
        res_columns[i++]->insert(fetch.source_replica_path);
        res_columns[i++]->insert(fetch.source_replica_hostname);
        res_columns[i++]->insert(fetch.source_replica_port);
        res_columns[i++]->insert(fetch.interserver_scheme);
        res_columns[i++]->insert(fetch.uri);
        res_columns[i++]->insert(fetch.to_detached);
        res_columns[i++]->insert(fetch.thread_id);
    }
}

}
