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
        {"hostname", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())},
        {"event_date", std::make_shared<DataTypeDate>()},
        {"event_time_microseconds", std::make_shared<DataTypeDateTime64>(6)},
        {"id", std::make_shared<DataTypeString>()},
        {"name", std::make_shared<DataTypeString>()},
        {"base_backup_name", std::make_shared<DataTypeString>()},
        {"status", std::make_shared<DataTypeEnum8>(getBackupStatusEnumValues())},
        {"error", std::make_shared<DataTypeString>()},
        {"start_time", std::make_shared<DataTypeDateTime>()},
        {"end_time", std::make_shared<DataTypeDateTime>()},
        {"num_files", std::make_shared<DataTypeUInt64>()},
        {"total_size", std::make_shared<DataTypeUInt64>()},
        {"num_entries", std::make_shared<DataTypeUInt64>()},
        {"uncompressed_size", std::make_shared<DataTypeUInt64>()},
        {"compressed_size", std::make_shared<DataTypeUInt64>()},
        {"files_read", std::make_shared<DataTypeUInt64>()},
        {"bytes_read", std::make_shared<DataTypeUInt64>()},
    };
}

void BackupLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t i = 0;
    columns[i++]->insert(getFQDNOrHostName());
    columns[i++]->insert(DateLUT::instance().toDayNum(std::chrono::system_clock::to_time_t(event_time)).toUnderType());
    columns[i++]->insert(event_time_usec);
    columns[i++]->insert(info.id);
    columns[i++]->insert(info.name);
    columns[i++]->insert(info.base_backup_name);
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
