#include <Storages/System/StorageSystemRoles.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeUUID.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Access/AccessControlManager.h>
#include <Access/Role.h>
#include <Access/AccessFlags.h>
#include <Interpreters/Context.h>


namespace DB
{

NamesAndTypesList StorageSystemRoles::getNamesAndTypes()
{
    NamesAndTypesList names_and_types{
        {"name", std::make_shared<DataTypeString>()},
        {"id", std::make_shared<DataTypeUUID>()},
        {"storage", std::make_shared<DataTypeString>()},
    };
    return names_and_types;
}


void StorageSystemRoles::fillData(MutableColumns & res_columns, const Context & context, const SelectQueryInfo &) const
{
    context.checkAccess(AccessType::SHOW_ROLES);
    const auto & access_control = context.getAccessControlManager();
    std::vector<UUID> ids = access_control.findAll<Role>();

    size_t column_index = 0;
    auto & column_name = assert_cast<ColumnString &>(*res_columns[column_index++]);
    auto & column_id = assert_cast<ColumnUInt128 &>(*res_columns[column_index++]).getData();
    auto & column_storage = assert_cast<ColumnString &>(*res_columns[column_index++]);

    auto add_row = [&](const String & name,
                       const UUID & id,
                       const String & storage_name)
    {
        column_name.insertData(name.data(), name.length());
        column_id.push_back(id);
        column_storage.insertData(storage_name.data(), storage_name.length());
    };

    for (const auto & id : ids)
    {
        auto role = access_control.tryRead<Role>(id);
        if (!role)
            continue;

        const auto * storage = access_control.findStorage(id);
        if (!storage)
            continue;

        add_row(role->getName(), id, storage->getStorageName());
    }
}

}
