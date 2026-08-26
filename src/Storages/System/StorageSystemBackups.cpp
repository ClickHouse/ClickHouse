#include <Storages/System/StorageSystemBackups.h>
#include <Common/SystemTableDocumentation.h>
#include <Storages/System/SystemTableSourceRegistry.h>
#include <Backups/BackupsWorker.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypeMap.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnString.h>
#include <Core/Field.h>
#include <Columns/ColumnsNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProfileEventsExt.h>
#include <Columns/ColumnsDateTime.h>


namespace DB
{

ColumnsDescription StorageSystemBackups::getColumnsDescription()
{
    auto low_cardinality_string = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());

    return ColumnsDescription
    {
        {"id", std::make_shared<DataTypeString>(), "Operation ID, can be either passed via SETTINGS id=... or be randomly generated UUID."},
        {"name", std::make_shared<DataTypeString>(), "Operation name, a string like `Disk('backups', 'my_backup')`"},
        {"base_backup_name", std::make_shared<DataTypeString>(), "Base Backup Operation name, a string like `Disk('backups', 'my_base_backup')`"},
        {"query_id", std::make_shared<DataTypeString>(), "Query ID of a query that started backup."},
        {"status", std::make_shared<DataTypeEnum8>(getBackupStatusEnumValues()), "Status of backup or restore operation."},
        {"error", std::make_shared<DataTypeString>(), "The error message if any."},
        {"start_time", std::make_shared<DataTypeDateTime64>(6), "The time when operation started."},
        {"end_time", std::make_shared<DataTypeDateTime64>(6), "The time when operation finished."},
        {"num_files", std::make_shared<DataTypeUInt64>(), "The number of files stored in the backup."},
        {"total_size", std::make_shared<DataTypeUInt64>(), "The total size of files stored in the backup."},
        {"num_entries", std::make_shared<DataTypeUInt64>(), "The number of entries in the backup, i.e. the number of files inside the folder if the backup is stored as a folder."},
        {"uncompressed_size", std::make_shared<DataTypeUInt64>(), "The uncompressed size of the backup."},
        {"compressed_size", std::make_shared<DataTypeUInt64>(), "The compressed size of the backup."},
        {"files_read", std::make_shared<DataTypeUInt64>(), "Returns the number of files read during RESTORE from this backup."},
        {"bytes_read", std::make_shared<DataTypeUInt64>(), "Returns the total size of files read during RESTORE from this backup."},
        {"ProfileEvents", std::make_shared<DataTypeMap>(low_cardinality_string, std::make_shared<DataTypeUInt64>()), "All the profile events captured during this operation."},
        {"settings", std::make_shared<DataTypeMap>(low_cardinality_string, std::make_shared<DataTypeString>()), "Backup/restore-specific settings effectively used for this operation (from the `SETTINGS` clause, including defaults). Sensitive settings are not exposed."},
        {"engine_settings", std::make_shared<DataTypeMap>(low_cardinality_string, std::make_shared<DataTypeString>()), "Settings effectively used by the backup engine's reader/writer (e.g. S3 `allow_native_copy`). Empty when the operation involves more than one engine that a flat map cannot represent: incremental backups and restores, lightweight snapshot restores, and non-internal `ON CLUSTER` operations."},
    };
}


void StorageSystemBackups::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    size_t column_index = 0;
    auto & column_id = assert_cast<ColumnString &>(*res_columns[column_index++]);
    auto & column_name = assert_cast<ColumnString &>(*res_columns[column_index++]);
    auto & column_base_backup_name = assert_cast<ColumnString &>(*res_columns[column_index++]);
    auto & column_query_id = assert_cast<ColumnString &>(*res_columns[column_index++]);
    auto & column_status = assert_cast<ColumnInt8 &>(*res_columns[column_index++]);
    auto & column_error = assert_cast<ColumnString &>(*res_columns[column_index++]);
    auto & column_start_time = assert_cast<ColumnDateTime64 &>(*res_columns[column_index++]);
    auto & column_end_time = assert_cast<ColumnDateTime64 &>(*res_columns[column_index++]);
    auto & column_num_files = assert_cast<ColumnUInt64 &>(*res_columns[column_index++]);
    auto & column_total_size = assert_cast<ColumnUInt64 &>(*res_columns[column_index++]);
    auto & column_num_entries = assert_cast<ColumnUInt64 &>(*res_columns[column_index++]);
    auto & column_uncompressed_size = assert_cast<ColumnUInt64 &>(*res_columns[column_index++]);
    auto & column_compressed_size = assert_cast<ColumnUInt64 &>(*res_columns[column_index++]);
    auto & column_num_read_files = assert_cast<ColumnUInt64 &>(*res_columns[column_index++]);
    auto & column_num_read_bytes = assert_cast<ColumnUInt64 &>(*res_columns[column_index++]);
    auto & column_profile_events = assert_cast<ColumnMap &>(*res_columns[column_index++]);
    auto & column_settings = assert_cast<ColumnMap &>(*res_columns[column_index++]);
    auto & column_engine_settings = assert_cast<ColumnMap &>(*res_columns[column_index++]);

    auto add_string_map = [](ColumnMap & column_map, const std::map<String, String> & map)
    {
        Map map_field;
        map_field.reserve(map.size());
        for (const auto & [key, value] : map)
            map_field.push_back(Tuple{key, value});
        column_map.insert(map_field);
    };

    auto add_row = [&](const BackupOperationInfo & info)
    {
        column_id.insertData(info.id.data(), info.id.size());
        column_name.insertData(info.name.data(), info.name.size());
        column_base_backup_name.insertData(info.base_backup_name.data(), info.base_backup_name.size());
        column_query_id.insertData(info.query_id.data(), info.query_id.size());
        column_status.insertValue(static_cast<Int8>(info.status));
        column_error.insertData(info.error_message.data(), info.error_message.size());
        column_start_time.insertValue(static_cast<Decimal64>(info.start_time_us));
        column_end_time.insertValue(static_cast<Decimal64>(info.end_time_us));
        column_num_files.insertValue(info.num_files);
        column_total_size.insertValue(info.total_size);
        column_num_entries.insertValue(info.num_entries);
        column_uncompressed_size.insertValue(info.uncompressed_size);
        column_compressed_size.insertValue(info.compressed_size);
        column_num_read_files.insertValue(info.num_read_files);
        column_num_read_bytes.insertValue(info.num_read_bytes);
        if (info.profile_counters)
            ProfileEvents::dumpToMapColumn(*info.profile_counters, &column_profile_events, true);
        else
            column_profile_events.insertDefault();
        add_string_map(column_settings, info.settings);
        add_string_map(column_engine_settings, info.engine_settings);
    };

    for (const auto & entry : context->getBackupsWorker().getAllInfos())
        add_row(entry);
}

}

/// Register the source file of this system table for `system.documentation`.
namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemBackups) }

namespace DB
{

REGISTER_SYSTEM_TABLE_DOCUMENTATION(
    "backups",
    .description = R"DOCS_MD(
Contains a list of all `BACKUP` or `RESTORE` operations with their current states and other properties. Note, that table is not persistent and it shows only operations executed after the last server restart.

## Restore atomicity {#restore-atomicity}

`RESTORE` is not transactional and does not roll back on failure. For each table, all selected parts are copied before any are attached, but the attach phase itself is not transactional — parts are made visible one at a time. Tables are processed independently.

**Tables are independent.** A table whose restore completes stays in place even if another table in the same command later fails:

```sql
RESTORE TABLE db.t0, TABLE db.t1
FROM S3('<endpoint>', '<access_key>', '<secret_key>')
SETTINGS
    allow_non_empty_tables = true;
```

If this command fails after `db.t0` has been fully restored but `db.t1` has not finished, `db.t0` remains restored.

**The `PARTITIONS` clause is not a commit boundary.** It only selects which parts of a table are restored:

```sql
RESTORE TABLE db.t0 PARTITIONS '2026-06-01', '2026-06-02', '2026-06-03'
FROM S3('<endpoint>', '<access_key>', '<secret_key>')
SETTINGS
    allow_non_empty_tables = true;
```

All selected parts of the table are copied first and attached only once every one of them is ready. So if this command fails during the copy phase — e.g. after partition `2026-06-01` has been fully copied but `2026-06-02` and `2026-06-03` have not finished — then `2026-06-01` is **not** committed and the table is left with no restored data from this command. Once the copy phase completes and the attach step begins, parts are committed one at a time, so a failure during attach can leave the table partially restored, without rollback.

To commit partitions independently (so a completed partition survives a later failure and can be retried in isolation), run a separate `RESTORE` per partition, using `SETTINGS allow_non_empty_tables = true` after the first.
)DOCS_MD")

}
