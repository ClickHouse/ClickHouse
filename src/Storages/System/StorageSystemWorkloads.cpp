#include <Columns/IColumn.h>
#include <Common/SystemTableDocumentation.h>
#include <Storages/System/SystemTableSourceRegistry.h>
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
    /// Hold a shared_ptr to keep the storage alive for the duration of this call, in case of concurrent shutdown.
    auto storage = context->getWorkloadEntityStoragePtr();
    const auto & entities = storage->getAllEntities();
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

/// Register the source file of this system table for `system.documentation`.
namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemWorkloads) }

namespace DB
{

REGISTER_SYSTEM_TABLE_DOCUMENTATION(
    "workloads",
    .description = R"DOCS_MD(
Contains information for [workloads](/concepts/features/configuration/server-config/workload-scheduling#workload_entity_storage) residing on the local server. The table contains a row for every workload.
)DOCS_MD",
    .examples = R"DOCS_MD(
```sql
SELECT *
FROM system.workloads
FORMAT Vertical
```

```text
Row 1:
──────
name:         production
parent:       all
create_query: CREATE WORKLOAD production IN `all` SETTINGS weight = 9

Row 2:
──────
name:         development
parent:       all
create_query: CREATE WORKLOAD development IN `all`

Row 3:
──────
name:         all
parent:
create_query: CREATE WORKLOAD `all`
```
)DOCS_MD")

}
