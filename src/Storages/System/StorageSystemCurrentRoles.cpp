#include <Storages/System/StorageSystemCurrentRoles.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Access/User.h>
#include <Access/EnabledRolesInfo.h>
#include <Interpreters/Context.h>


namespace DB
{

NamesAndTypesList StorageSystemCurrentRoles::getNamesAndTypes()
{
    NamesAndTypesList names_and_types{
        {"role_name", std::make_shared<DataTypeString>()},
        {"with_admin_option", std::make_shared<DataTypeUInt8>()},
        {"is_default", std::make_shared<DataTypeUInt8>()},
    };
    return names_and_types;
}


void StorageSystemCurrentRoles::fillData(MutableColumns & res_columns, const Context & context, const SelectQueryInfo &) const
{
    auto roles_info = context.getRolesInfo();
    auto user = context.getUser();
    if (!roles_info || !user)
        return;

    size_t column_index = 0;
    auto & column_role_name = assert_cast<ColumnString &>(*res_columns[column_index++]);
    auto & column_admin_option = assert_cast<ColumnUInt8 &>(*res_columns[column_index++]).getData();
    auto & column_is_default = assert_cast<ColumnUInt8 &>(*res_columns[column_index++]).getData();

    auto add_row = [&](const String & role_name, bool admin_option, bool is_default)
    {
        column_role_name.insertData(role_name.data(), role_name.length());
        column_admin_option.push_back(admin_option);
        column_is_default.push_back(is_default);
    };

    for (const auto & role_id : roles_info->current_roles)
    {
        const String & role_name = roles_info->names_of_roles.at(role_id);
        bool admin_option = roles_info->enabled_roles_with_admin_option.count(role_id);
        bool is_default = user->default_roles.match(role_id);
        add_row(role_name, admin_option, is_default);
    }
}

}
