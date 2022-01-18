#include <Storages/System/StorageSystemReplicatedFetches.h>
#include <Storages/MergeTree/ReplicatedFetchList.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Access/ContextAccess.h>

namespace DB
{

NamesAndTypesList StorageSystemReplicatedFetches::getNamesAndTypes()
{
    return {
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
    };
}

void StorageSystemReplicatedFetches::fillData(MutableColumns & res_columns, const Context & context, const SelectQueryInfo &) const
{
    const auto access = context.getAccess();
    const bool check_access_for_tables = !access->isGranted(AccessType::SHOW_TABLES);

    for (const auto & fetch : context.getReplicatedFetchList().get())
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
