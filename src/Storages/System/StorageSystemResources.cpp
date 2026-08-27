#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
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
        {"unit", std::make_shared<DataTypeString>(), "Resource unit used for cost measurements."},
        {"create_query", std::make_shared<DataTypeString>(), "CREATE query of the resource."},
    };
}

void StorageSystemResources::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    const auto & storage = context->getWorkloadEntityStorage();
    const auto & entities = storage.getAllEntities();
    for (const auto & [name, ast] : entities)
    {
        if (auto * resource = typeid_cast<ASTCreateResourceQuery *>(ast.get()))
        {
            res_columns[0]->insert(name);
            {
                Array read_disks;
                Array write_disks;
                for (const auto & [mode, disk] : resource->operations)
                {
                    switch (mode)
                    {
                        case DB::ResourceAccessMode::DiskRead:
                        {
                            read_disks.emplace_back(disk ? *disk : "ANY");
                            break;
                        }
                        case DB::ResourceAccessMode::DiskWrite:
                        {
                            write_disks.emplace_back(disk ? *disk : "ANY");
                            break;
                        }
                        default: // Ignore
                    }
                }
                res_columns[1]->insert(read_disks);
                res_columns[2]->insert(write_disks);
            }
            res_columns[3]->insert(DB::costUnitToString(resource->unit));
            res_columns[4]->insert(ast->formatForLogging());
        }
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
