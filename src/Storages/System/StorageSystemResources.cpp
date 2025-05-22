#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeArray.h>
#include <Interpreters/Context.h>
#include <Parsers/queryToString.h>
#include <Storages/System/StorageSystemResources.h>
#include <Common/Scheduler/Workload/IWorkloadEntityStorage.h>
#include <Parsers/ASTCreateResourceQuery.h>


namespace DB
{

ColumnsDescription StorageSystemResources::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"name", std::make_shared<DataTypeString>(), "The name of the resource."},
        {"read_disks", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "The list of disk names that uses this resource for read operations."},
        {"write_disks", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "The list of disk names that uses this resource for write operations."},
        {"create_query", std::make_shared<DataTypeString>(), "CREATE query of the resource."},
    };
}

void StorageSystemResources::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    const auto & storage = context->getWorkloadEntityStorage();
    const auto & resource_names = storage.getAllEntityNames(WorkloadEntityType::Resource);
    for (const auto & resource_name : resource_names)
    {
        auto ast = storage.get(resource_name);
        auto & resource = typeid_cast<ASTCreateResourceQuery &>(*ast);
        res_columns[0]->insert(resource_name);
        {
            Array read_disks;
            Array write_disks;
            for (const auto & [mode, disk] : resource.operations)
            {
                switch (mode)
                {
                    case DB::ASTCreateResourceQuery::AccessMode::Read:
                    {
                        read_disks.emplace_back(disk ? *disk : "ANY");
                        break;
                    }
                    case DB::ASTCreateResourceQuery::AccessMode::Write:
                    {
                        write_disks.emplace_back(disk ? *disk : "ANY");
                        break;
                    }
                }
            }
            res_columns[1]->insert(read_disks);
            res_columns[2]->insert(write_disks);
        }
        res_columns[3]->insert(queryToString(ast));
    }
}

void StorageSystemResources::backupData(BackupEntriesCollector & /*backup_entries_collector*/, const String & /*data_path_in_backup*/, const std::optional<ASTs> & /* partitions */)
{
    // TODO(serxa): add backup for resources
    // storage.backup(backup_entries_collector, data_path_in_backup);
}

void StorageSystemResources::restoreDataFromBackup(RestorerFromBackup & /*restorer*/, const String & /*data_path_in_backup*/, const std::optional<ASTs> & /* partitions */)
{
    // TODO(serxa): add restore for resources
    // storage.restore(restorer, data_path_in_backup);
}

}
