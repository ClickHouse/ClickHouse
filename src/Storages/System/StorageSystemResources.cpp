#include <DataTypes/DataTypeString.h>
#include <Common/SystemTableDocumentation.h>
#include <Storages/System/SystemTableSourceRegistry.h>
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
    /// Hold a shared_ptr to keep the storage alive for the duration of this call, in case of concurrent shutdown.
    auto storage = context->getWorkloadEntityStoragePtr();
    const auto & entities = storage->getAllEntities();
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

/// Register the source file of this system table for `system.documentation`.
namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemResources) }

namespace DB
{

REGISTER_SYSTEM_TABLE_DOCUMENTATION(
    "resources",
    .description = R"DOCS_MD(
Contains information about [resources](/concepts/features/configuration/server-config/workload-scheduling#workload_entity_storage) residing on the local server. The table contains a row for every resource.
)DOCS_MD",
    .examples = R"DOCS_MD(
```sql
SELECT *
FROM system.resources
FORMAT Vertical
```

```text
Row 1:
──────
name:         io_read
read_disks:   ['s3']
write_disks:  []
create_query: CREATE RESOURCE io_read (READ DISK s3)

Row 2:
──────
name:         io_write
read_disks:   []
write_disks:  ['s3']
create_query: CREATE RESOURCE io_write (WRITE DISK s3)
```
)DOCS_MD")

}
