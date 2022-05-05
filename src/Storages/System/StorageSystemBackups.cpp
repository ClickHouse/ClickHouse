#include <Storages/System/StorageSystemBackups.h>
#include <Backups/BackupsWorker.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>


namespace DB
{

NamesAndTypesList StorageSystemBackups::getNamesAndTypes()
{
    NamesAndTypesList names_and_types{
        {"task_id", std::make_shared<DataTypeUInt64>()},
        {"backup_name", std::make_shared<DataTypeString>()},
        {"status", std::make_shared<DataTypeEnum8>(getBackupStatusEnumValues())},
        {"error", std::make_shared<DataTypeString>()},
        {"time", std::make_shared<DataTypeDateTime>()},
    };
    return names_and_types;
}


void StorageSystemBackups::fillData(MutableColumns & res_columns, ContextPtr, const SelectQueryInfo &) const
{
    size_t column_index = 0;
    auto & column_task_id = assert_cast<ColumnUInt64 &>(*res_columns[column_index++]);
    auto & column_backup_name = assert_cast<ColumnString &>(*res_columns[column_index++]);
    auto & column_status = assert_cast<ColumnInt8 &>(*res_columns[column_index++]);
    auto & column_error = assert_cast<ColumnString &>(*res_columns[column_index++]);
    auto & column_timestamp = assert_cast<ColumnUInt32 &>(*res_columns[column_index++]);

    auto add_row = [&](const BackupsWorker::Entry & entry)
    {
        column_task_id.insertValue(entry.task_id);
        column_backup_name.insertData(entry.backup_name.data(), entry.backup_name.size());
        column_status.insertValue(static_cast<Int8>(entry.status));
        column_error.insertData(entry.error.data(), entry.error.size());
        column_timestamp.insertValue(entry.timestamp);
    };

    for (const auto & entry : BackupsWorker::instance().getEntries())
        add_row(entry);
}

}
