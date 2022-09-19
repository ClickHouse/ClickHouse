#include <Storages/System/StorageSystemBackups.h>
#include <Backups/BackupsWorker.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeUUID.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Interpreters/Context.h>


namespace DB
{

NamesAndTypesList StorageSystemBackups::getNamesAndTypes()
{
    NamesAndTypesList names_and_types{
        {"uuid", std::make_shared<DataTypeUUID>()},
        {"backup_name", std::make_shared<DataTypeString>()},
        {"status", std::make_shared<DataTypeEnum8>(getBackupStatusEnumValues())},
        {"status_changed_time", std::make_shared<DataTypeDateTime>()},
        {"error", std::make_shared<DataTypeString>()},
        {"internal", std::make_shared<DataTypeUInt8>()},
    };
    return names_and_types;
}


void StorageSystemBackups::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const
{
    size_t column_index = 0;
    auto & column_uuid = assert_cast<ColumnUUID &>(*res_columns[column_index++]);
    auto & column_backup_name = assert_cast<ColumnString &>(*res_columns[column_index++]);
    auto & column_status = assert_cast<ColumnInt8 &>(*res_columns[column_index++]);
    auto & column_status_changed_time = assert_cast<ColumnUInt32 &>(*res_columns[column_index++]);
    auto & column_error = assert_cast<ColumnString &>(*res_columns[column_index++]);
    auto & column_internal = assert_cast<ColumnUInt8 &>(*res_columns[column_index++]);

    auto add_row = [&](const BackupsWorker::Info & info)
    {
        column_uuid.insertValue(info.uuid);
        column_backup_name.insertData(info.backup_name.data(), info.backup_name.size());
        column_status.insertValue(static_cast<Int8>(info.status));
        column_status_changed_time.insertValue(info.status_changed_time);
        column_error.insertData(info.error_message.data(), info.error_message.size());
        column_internal.insertValue(info.internal);
    };

    for (const auto & entry : context->getBackupsWorker().getAllInfos())
        add_row(entry);
}

}
