#include <Storages/System/StorageSystemRoleGrants.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Access/AccessControl.h>
#include <Access/Role.h>
#include <Access/User.h>
#include <Interpreters/Context.h>
#include <boost/range/algorithm_ext/push_back.hpp>


namespace DB
{

NamesAndTypesList StorageSystemRoleGrants::getNamesAndTypes()
{
    NamesAndTypesList names_and_types{
        {"user_name", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>())},
        {"role_name", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>())},
        {"granted_role_name", std::make_shared<DataTypeString>()},
        {"granted_role_is_default", std::make_shared<DataTypeUInt8>()},
        {"with_admin_option", std::make_shared<DataTypeUInt8>()},
    };
    return names_and_types;
}


void StorageSystemRoleGrants::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const
{
    /// If "select_from_system_db_requires_grant" is enabled the access rights were already checked in InterpreterSelectQuery.
    const auto & access_control = context->getAccessControl();
    if (!access_control.doesSelectFromSystemDatabaseRequireGrant())
        context->checkAccess(AccessType::SHOW_USERS | AccessType::SHOW_ROLES);

    std::vector<UUID> ids = access_control.findAll<User>();
    boost::range::push_back(ids, access_control.findAll<Role>());

    size_t column_index = 0;
    auto & column_user_name = assert_cast<ColumnString &>(assert_cast<ColumnNullable &>(*res_columns[column_index]).getNestedColumn());
    auto & column_user_name_null_map = assert_cast<ColumnNullable &>(*res_columns[column_index++]).getNullMapData();
    auto & column_role_name = assert_cast<ColumnString &>(assert_cast<ColumnNullable &>(*res_columns[column_index]).getNestedColumn());
    auto & column_role_name_null_map = assert_cast<ColumnNullable &>(*res_columns[column_index++]).getNullMapData();
    auto & column_granted_role_name = assert_cast<ColumnString &>(*res_columns[column_index++]);
    auto & column_is_default = assert_cast<ColumnUInt8 &>(*res_columns[column_index++]).getData();
    auto & column_admin_option = assert_cast<ColumnUInt8 &>(*res_columns[column_index++]).getData();

    auto add_row = [&](const String & grantee_name,
                       AccessEntityType grantee_type,
                       const String & granted_role_name,
                       bool is_default,
                       bool with_admin_option)
    {
        if (grantee_type == AccessEntityType::USER)
        {
            column_user_name.insertData(grantee_name.data(), grantee_name.length());
            column_user_name_null_map.push_back(false);
            column_role_name.insertDefault();
            column_role_name_null_map.push_back(true);
        }
        else if (grantee_type == AccessEntityType::ROLE)
        {
            column_user_name.insertDefault();
            column_user_name_null_map.push_back(true);
            column_role_name.insertData(grantee_name.data(), grantee_name.length());
            column_role_name_null_map.push_back(false);
        }
        else
            assert(false);

        column_granted_role_name.insertData(granted_role_name.data(), granted_role_name.length());
        column_is_default.push_back(is_default);
        column_admin_option.push_back(with_admin_option);
    };

    auto add_rows = [&](const String & grantee_name,
                        AccessEntityType grantee_type,
                        const GrantedRoles & granted_roles,
                        const RolesOrUsersSet * default_roles)
    {
        for (const auto & element : granted_roles.getElements())
        {
            for (const auto & role_id : element.ids)
            {
                auto role_name = access_control.tryReadName(role_id);
                if (!role_name)
                    continue;

                bool is_default = !default_roles || default_roles->match(role_id);
                add_row(grantee_name, grantee_type, *role_name, is_default, element.admin_option);
            }
        }
    };

    for (const auto & id : ids)
    {
        auto entity = access_control.tryRead(id);
        if (!entity)
            continue;

        const GrantedRoles * granted_roles = nullptr;
        const RolesOrUsersSet * default_roles = nullptr;
        if (auto role = typeid_cast<RolePtr>(entity))
            granted_roles = &role->granted_roles;
        else if (auto user = typeid_cast<UserPtr>(entity))
        {
            granted_roles = &user->granted_roles;
            default_roles = &user->default_roles;
        }
        else
            continue;

        add_rows(entity->getName(), entity->getType(), *granted_roles, default_roles);
    }
}

}
