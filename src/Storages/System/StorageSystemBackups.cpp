#include <Storages/System/StorageSystemBackups.h>
#include <Backups/BackupsWorker.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypeMap.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProfileEventsExt.h>


namespace DB
{

ColumnsDescription StorageSystemBackups::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"id", std::make_shared<DataTypeString>(), "Operation ID, can be either passed via SETTINGS id=... or be randomly generated UUID."},
        {"name", std::make_shared<DataTypeString>(), "Operation name, a string like `Disk('backups', 'my_backup')`"},
        {"base_backup_name", std::make_shared<DataTypeString>(), "Base Backup Operation name, a string like `Disk('backups', 'my_base_backup')`"},
        {"query_id", std::make_shared<DataTypeString>(), "Query ID of a query that started backup."},
        {"status", std::make_shared<DataTypeEnum8>(getBackupStatusEnumValues()), "Status of backup or restore operation."},
        {"error", std::make_shared<DataTypeString>(), "The error message if any."},
        {"start_time", std::make_shared<DataTypeDateTime>(), "The time when operation started."},
        {"end_time", std::make_shared<DataTypeDateTime>(), "The time when operation finished."},
        {"num_files", std::make_shared<DataTypeUInt64>(), "The number of files stored in the backup."},
        {"total_size", std::make_shared<DataTypeUInt64>(), "The total size of files stored in the backup."},
        {"num_entries", std::make_shared<DataTypeUInt64>(), "The number of entries in the backup, i.e. the number of files inside the folder if the backup is stored as a folder."},
        {"uncompressed_size", std::make_shared<DataTypeUInt64>(), "The uncompressed size of the backup."},
        {"compressed_size", std::make_shared<DataTypeUInt64>(), "The compressed size of the backup."},
        {"files_read", std::make_shared<DataTypeUInt64>(), "Returns the number of files read during RESTORE from this backup."},
        {"bytes_read", std::make_shared<DataTypeUInt64>(), "Returns the total size of files read during RESTORE from this backup."},
        {"ProfileEvents", std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeUInt64>()), "All the profile events captured during this operation."},
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
    auto & column_start_time = assert_cast<ColumnUInt32 &>(*res_columns[column_index++]);
    auto & column_end_time = assert_cast<ColumnUInt32 &>(*res_columns[column_index++]);
    auto & column_num_files = assert_cast<ColumnUInt64 &>(*res_columns[column_index++]);
    auto & column_total_size = assert_cast<ColumnUInt64 &>(*res_columns[column_index++]);
    auto & column_num_entries = assert_cast<ColumnUInt64 &>(*res_columns[column_index++]);
    auto & column_uncompressed_size = assert_cast<ColumnUInt64 &>(*res_columns[column_index++]);
    auto & column_compressed_size = assert_cast<ColumnUInt64 &>(*res_columns[column_index++]);
    auto & column_num_read_files = assert_cast<ColumnUInt64 &>(*res_columns[column_index++]);
    auto & column_num_read_bytes = assert_cast<ColumnUInt64 &>(*res_columns[column_index++]);
    auto & column_profile_events = assert_cast<ColumnMap &>(*res_columns[column_index++]);

    auto add_row = [&](const BackupOperationInfo & info)
    {
        column_id.insertData(info.id.data(), info.id.size());
        column_name.insertData(info.name.data(), info.name.size());
        column_base_backup_name.insertData(info.base_backup_name.data(), info.base_backup_name.size());
        column_query_id.insertData(info.query_id.data(), info.query_id.size());
        column_status.insertValue(static_cast<Int8>(info.status));
        column_error.insertData(info.error_message.data(), info.error_message.size());
        column_start_time.insertValue(static_cast<UInt32>(std::chrono::system_clock::to_time_t(info.start_time)));
        column_end_time.insertValue(static_cast<UInt32>(std::chrono::system_clock::to_time_t(info.end_time)));
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
    };

    for (const auto & entry : context->getBackupsWorker().getAllInfos())
        add_row(entry);
}

}
