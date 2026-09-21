#include <Storages/System/StorageSystemEnabledRoles.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Access/User.h>
#include <Access/EnabledRolesInfo.h>
#include <Interpreters/Context.h>


namespace DB
{

ColumnsDescription StorageSystemEnabledRoles::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"role_name", std::make_shared<DataTypeString>(), "Role name."},
        {"with_admin_option", std::make_shared<DataTypeUInt8>(), "1 if the role has ADMIN OPTION privilege."},
        {"is_current", std::make_shared<DataTypeUInt8>(), "Flag that shows whether `enabled_role` is a current role of a current user."},
        {"is_default", std::make_shared<DataTypeUInt8>(), "Flag that shows whether `enabled_role` is a default role."},
    };
}


void StorageSystemEnabledRoles::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    auto roles_info = context->getRolesInfo();
    auto user = context->getUser();

    size_t column_index = 0;
    auto & column_role_name = assert_cast<ColumnString &>(*res_columns[column_index++]);
    auto & column_admin_option = assert_cast<ColumnUInt8 &>(*res_columns[column_index++]).getData();
    auto & column_is_current = assert_cast<ColumnUInt8 &>(*res_columns[column_index++]).getData();
    auto & column_is_default = assert_cast<ColumnUInt8 &>(*res_columns[column_index++]).getData();

    auto add_row = [&](const String & role_name, bool admin_option, bool is_current, bool is_default)
    {
        column_role_name.insertData(role_name.data(), role_name.length());
        column_admin_option.push_back(admin_option);
        column_is_current.push_back(is_current);
        column_is_default.push_back(is_default);
    };

    for (const auto & role_id : roles_info->enabled_roles)
    {
        const String & role_name = roles_info->names_of_roles.at(role_id);
        bool admin_option = roles_info->enabled_roles_with_admin_option.count(role_id);
        bool is_current = roles_info->current_roles.count(role_id);
        bool is_default = user->default_roles.match(role_id);
        add_row(role_name, admin_option, is_current, is_default);
    }
}

}
