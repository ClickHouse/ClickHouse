#include <Columns/IColumn.h>
#include <DataTypes/DataTypeString.h>
#include <Interpreters/Context.h>
#include <Storages/System/StorageSystemWorkloads.h>
#include <Common/Scheduler/Workload/IWorkloadEntityStorage.h>
#include <Parsers/ASTCreateWorkloadQuery.h>


namespace DB
{

ColumnsDescription StorageSystemWorkloads::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"name", std::make_shared<DataTypeString>(), "The name of the workload."},
        {"parent", std::make_shared<DataTypeString>(), "The name of the parent workload."},
        {"create_query", std::make_shared<DataTypeString>(), "CREATE query of the workload."},
    };
}

void StorageSystemWorkloads::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    const auto & storage = context->getWorkloadEntityStorage();
    const auto & entities = storage.getAllEntities();
    for (const auto & [name, ast] : entities)
    {
        if (auto * workload = typeid_cast<ASTCreateWorkloadQuery *>(ast.get()))
        {
            res_columns[0]->insert(name);
            res_columns[1]->insert(workload->getWorkloadParent());
            res_columns[2]->insert(ast->formatForLogging());
        }
    }
}

void StorageSystemWorkloads::backupData(BackupEntriesCollector & /*backup_entries_collector*/, const String & /*data_path_in_backup*/, const std::optional<ASTs> & /* partitions */)
{
    // TODO(serxa): add backup for workloads
    // storage.backup(backup_entries_collector, data_path_in_backup);
}

void StorageSystemWorkloads::restoreDataFromBackup(RestorerFromBackup & /*restorer*/, const String & /*data_path_in_backup*/, const std::optional<ASTs> & /* partitions */)
{
    // TODO(serxa): add restore for workloads
    // storage.restore(restorer, data_path_in_backup);
}

}
