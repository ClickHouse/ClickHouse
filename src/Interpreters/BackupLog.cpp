#include <Interpreters/BackupLog.h>

#include <base/getFQDNOrHostName.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>

namespace DB
{

BackupLogElement::BackupLogElement(BackupOperationInfo info_)
    : event_time(std::chrono::system_clock::now())
    , event_time_usec(timeInMicroseconds(event_time))
    , info(std::move(info_))
{
}

ColumnsDescription BackupLogElement::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"hostname", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "Hostname of the server executing the query."},
        {"event_date", std::make_shared<DataTypeDate>(), "Date of the entry."},
        {"event_time", std::make_shared<DataTypeDateTime>(), "Time of the entry."},
        {"event_time_microseconds", std::make_shared<DataTypeDateTime64>(6), "Time of the entry with microseconds precision."},
        {"id", std::make_shared<DataTypeString>(), "Identifier of the backup or restore operation."},
        {"name", std::make_shared<DataTypeString>(), "Name of the backup storage (the contents of the FROM or TO clause)."},
        {"base_backup_name", std::make_shared<DataTypeString>(), "The name of base backup in case incremental one."},
        {"query_id", std::make_shared<DataTypeString>(), "The ID of a query associated with a backup operation."},
        {"status", std::make_shared<DataTypeEnum8>(getBackupStatusEnumValues()), "Operation status."},
        {"error", std::make_shared<DataTypeString>(), "Error message of the failed operation (empty string for successful operations)."},
        {"start_time", std::make_shared<DataTypeDateTime>(), "Start time of the operation."},
        {"end_time", std::make_shared<DataTypeDateTime>(), "End time of the operation."},
        {"num_files", std::make_shared<DataTypeUInt64>(), "Number of files stored in the backup."},
        {"total_size", std::make_shared<DataTypeUInt64>(), "Total size of files stored in the backup."},
        {"num_entries", std::make_shared<DataTypeUInt64>(), "Number of entries in the backup, i.e. the number of files inside the folder if the backup is stored as a folder, or the number of files inside the archive if the backup is stored as an archive. It is not the same as num_files if it's an incremental backup or if it contains empty files or duplicates. The following is always true: num_entries <= num_files."},
        {"uncompressed_size", std::make_shared<DataTypeUInt64>(), "Uncompressed size of the backup."},
        {"compressed_size", std::make_shared<DataTypeUInt64>(), "Compressed size of the backup. If the backup is not stored as an archive it equals to uncompressed_size."},
        {"files_read", std::make_shared<DataTypeUInt64>(), "Number of files read during the restore operation."},
        {"bytes_read", std::make_shared<DataTypeUInt64>(), "Total size of files read during the restore operation."},
    };
}

void BackupLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t i = 0;
    columns[i++]->insert(getFQDNOrHostName());
    columns[i++]->insert(DateLUT::instance().toDayNum(std::chrono::system_clock::to_time_t(event_time)).toUnderType());
    columns[i++]->insert(std::chrono::system_clock::to_time_t(event_time));
    columns[i++]->insert(event_time_usec);
    columns[i++]->insert(info.id);
    columns[i++]->insert(info.name);
    columns[i++]->insert(info.base_backup_name);
    columns[i++]->insert(info.query_id);
    columns[i++]->insert(static_cast<Int8>(info.status));
    columns[i++]->insert(info.error_message);
    columns[i++]->insert(static_cast<UInt32>(std::chrono::system_clock::to_time_t(info.start_time)));
    columns[i++]->insert(static_cast<UInt32>(std::chrono::system_clock::to_time_t(info.end_time)));
    columns[i++]->insert(info.num_files);
    columns[i++]->insert(info.total_size);
    columns[i++]->insert(info.num_entries);
    columns[i++]->insert(info.uncompressed_size);
    columns[i++]->insert(info.compressed_size);
    columns[i++]->insert(info.num_read_files);
    columns[i++]->insert(info.num_read_bytes);
}

}
