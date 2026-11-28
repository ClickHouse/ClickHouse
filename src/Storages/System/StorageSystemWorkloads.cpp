#include <Columns/IColumn.h>
#include <Storages/System/SystemTableSourceRegistry.h>
#include <DataTypes/DataTypeString.h>
#include <Interpreters/Context.h>
#include <Storages/System/StorageSystemWorkloads.h>
#include <Common/Scheduler/Workload/IWorkloadEntityStorage.h>
#include <Parsers/ASTCreateWorkloadQuery.h>
#include <Backups/BackupEntriesCollector.h>
#include <Backups/RestorerFromBackup.h>


namespace DB
{

ColumnsDescription StorageSystemWorkloads::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"name", std::make_shared<DataTypeString>(), "The name of the workload."},
        {"parent", std::make_shared<DataTypeString>(), "The name of the parent workload."},
        {"create_query", std::make_shared<DataTypeString>(), "CREATE query of the workload. For an implicit workload (`source` = `implicit`), including the root workload with the empty name, it describes the workload and cannot be executed."},
        {"source", std::make_shared<DataTypeString>(), "The name of the storage that defines the workload: `configuration` for the server configuration, `disk` or `keeper` for SQL, `implicit` for workloads created implicitly by the server."},
    };
}

void StorageSystemWorkloads::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    /// Hold a shared_ptr to keep the storage alive for the duration of this call, in case of concurrent shutdown.
    auto storage = context->getWorkloadEntityStoragePtr();
    const auto & entities = storage->getAllEntities();
    for (const auto & [key, ast] : entities)
    {
        if (auto * workload = typeid_cast<ASTCreateWorkloadQuery *>(ast.get()))
        {
            res_columns[0]->insert(key.name);
            res_columns[1]->insert(workload->getWorkloadParent());
            res_columns[2]->insert(ast->formatForLogging());
            res_columns[3]->insert(storage->getEntitySource(key.type, key.name));
        }
    }
}

void StorageSystemWorkloads::backupData(BackupEntriesCollector & backup_entries_collector, const String & data_path_in_backup, const std::optional<ASTs> & /* partitions */)
{
    backup_entries_collector.getContext()->getWorkloadEntityStoragePtr()->backup(
        backup_entries_collector, data_path_in_backup, WorkloadEntityType::Workload);
}

void StorageSystemWorkloads::restoreDataFromBackup(RestorerFromBackup & restorer, const String & data_path_in_backup, const std::optional<ASTs> & /* partitions */)
{
    restorer.getContext()->getWorkloadEntityStoragePtr()->restore(restorer, data_path_in_backup, WorkloadEntityType::Workload);
}

}

/// Register the source file of this system table for `system.documentation`.
namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemWorkloads) }
