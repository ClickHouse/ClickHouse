#include <Interpreters/BackupLog.h>

#include <base/getFQDNOrHostName.h>
#include <Common/DateLUTImpl.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>

namespace DB
{

void BackupLogElement::fromInfo(BackupLogElement & element, const BackupOperationInfo & info)
{
    element.event_time = std::chrono::system_clock::now();
    element.event_time_usec = timeInMicroseconds(element.event_time);
    element.id = info.id;
    element.name_ = info.name;
    element.base_backup_name = info.base_backup_name;
    element.query_id = info.query_id;
    element.status = info.status;
    element.error_message = info.error_message;
    element.start_time_us = info.start_time_us;
    element.end_time_us = info.end_time_us;
    element.num_files = info.num_files;
    element.total_size = info.total_size;
    element.num_entries = info.num_entries;
    element.uncompressed_size = info.uncompressed_size;
    element.compressed_size = info.compressed_size;
    element.num_read_files = info.num_read_files;
    element.num_read_bytes = info.num_read_bytes;
    element.settings = info.settings;
    element.engine_settings = info.engine_settings;
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
        {"start_time", std::make_shared<DataTypeDateTime64>(6), "Start time of the operation."},
        {"end_time", std::make_shared<DataTypeDateTime64>(6), "End time of the operation."},
        {"num_files", std::make_shared<DataTypeUInt64>(), "Number of files stored in the backup."},
        {"total_size", std::make_shared<DataTypeUInt64>(), "Total size of files stored in the backup."},
        {"num_entries", std::make_shared<DataTypeUInt64>(), "Number of entries in the backup, i.e. the number of files inside the folder if the backup is stored as a folder, or the number of files inside the archive if the backup is stored as an archive. It is not the same as `num_files` if it's an incremental backup or if it contains empty files or duplicates. The following is always true: `num_entries ≤ num_files`."},
        {"uncompressed_size", std::make_shared<DataTypeUInt64>(), "Uncompressed size of the backup."},
        {"compressed_size", std::make_shared<DataTypeUInt64>(), "Compressed size of the backup. If the backup is not stored as an archive it equals to uncompressed_size."},
        {"files_read", std::make_shared<DataTypeUInt64>(), "Number of files read during the restore operation."},
        {"bytes_read", std::make_shared<DataTypeUInt64>(), "Total size of files read during the restore operation."},
        {"settings", std::make_shared<DataTypeMap>(std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), std::make_shared<DataTypeString>()), "Backup/restore-specific settings effectively used for this operation (from the `SETTINGS` clause, including defaults). Sensitive settings are not exposed."},
        {"engine_settings", std::make_shared<DataTypeMap>(std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), std::make_shared<DataTypeString>()), "Settings effectively used by the backup engine's reader/writer (e.g. S3 `allow_native_copy`). Empty when the operation involves more than one engine that a flat map cannot represent: incremental backups and restores, lightweight snapshot restores, and non-internal `ON CLUSTER` operations."},
    };
}

void BackupLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t i = 0;
    columns[i++]->insert(getFQDNOrHostName());
    columns[i++]->insert(DateLUT::instance().toDayNum(std::chrono::system_clock::to_time_t(event_time)).toUnderType());
    columns[i++]->insert(std::chrono::system_clock::to_time_t(event_time));
    columns[i++]->insert(event_time_usec);
    columns[i++]->insert(id);
    columns[i++]->insert(name_);
    columns[i++]->insert(base_backup_name);
    columns[i++]->insert(query_id);
    columns[i++]->insert(static_cast<Int8>(status));
    columns[i++]->insert(error_message);
    columns[i++]->insert(static_cast<Decimal64>(start_time_us));
    columns[i++]->insert(static_cast<Decimal64>(end_time_us));
    columns[i++]->insert(num_files);
    columns[i++]->insert(total_size);
    columns[i++]->insert(num_entries);
    columns[i++]->insert(uncompressed_size);
    columns[i++]->insert(compressed_size);
    columns[i++]->insert(num_read_files);
    columns[i++]->insert(num_read_bytes);

    auto to_map_field = [](const std::map<String, String> & map)
    {
        Map map_field;
        map_field.reserve(map.size());
        for (const auto & [key, value] : map)
            map_field.push_back(Tuple{key, value});
        return map_field;
    };
    columns[i++]->insert(to_map_field(settings));
    columns[i++]->insert(to_map_field(engine_settings));
}

}
