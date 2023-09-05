#include <Interpreters/BackupInfoElement.h>

#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>

namespace DB
{

BackupInfoElement::BackupInfoElement(BackupOperationInfo info_)
    : info(std::move(info_))
{
}

NamesAndTypesList BackupInfoElement::getNamesAndTypes()
{
    return
    {
        {"id", std::make_shared<DataTypeString>()},
        {"name", std::make_shared<DataTypeString>()},
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

void BackupInfoElement::appendToBlock(MutableColumns & columns, size_t first_column_index) const
{
    size_t i = first_column_index;
    columns[i++]->insert(info.id);
    columns[i++]->insert(info.name);
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
