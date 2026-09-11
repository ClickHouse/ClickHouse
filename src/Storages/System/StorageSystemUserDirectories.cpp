#include <Storages/System/StorageSystemUserDirectories.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Interpreters/Context.h>
#include <Access/AccessControl.h>


namespace DB
{
ColumnsDescription StorageSystemUserDirectories::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"name", std::make_shared<DataTypeString>(), "The name of the directory."},
        {"type", std::make_shared<DataTypeString>(), "The type of the access storage e.g. users.xml or replicated or memory etc."},
        {"params", std::make_shared<DataTypeString>(), "JSON with the parameters of the access storage."},
        {"precedence", std::make_shared<DataTypeUInt64>(), "The order in which this directory is declared in the config. The same order is used when ClickHouse tries to find a user or role."},
    };
}


void StorageSystemUserDirectories::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    const auto & access_control = context->getAccessControl();
    auto storages = access_control.getStorages();

    size_t column_index = 0;
    auto & column_name = assert_cast<ColumnString &>(*res_columns[column_index++]);
    auto & column_type = assert_cast<ColumnString &>(*res_columns[column_index++]);
    auto & column_params = assert_cast<ColumnString &>(*res_columns[column_index++]);
    auto & column_precedence = assert_cast<ColumnUInt64 &>(*res_columns[column_index++]);

    auto add_row = [&](const IAccessStorage & storage, size_t precedence)
    {
        const String & name = storage.getStorageName();
        std::string_view type = storage.getStorageType();
        String params = storage.getStorageParamsJSON();

        column_name.insertData(name.data(), name.length());
        column_type.insertData(type.data(), type.length());
        column_params.insertData(params.data(), params.length());
        column_precedence.insert(precedence);
    };

    for (size_t i = 0; i < storages.size(); ++i)
    {
        const auto & storage = storages[i];
        add_row(*storage, i + 1);
    }
}

}
