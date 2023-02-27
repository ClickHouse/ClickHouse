#include <Storages/System/StorageSystemGrants.h>
#include <Storages/System/StorageSystemPrivileges.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Access/AccessControl.h>
#include <Access/Common/AccessRightsElement.h>
#include <Access/Role.h>
#include <Access/User.h>
#include <Interpreters/Context.h>
#include <boost/range/algorithm_ext/push_back.hpp>


namespace DB
{

NamesAndTypesList StorageSystemGrants::getNamesAndTypes()
{
    NamesAndTypesList names_and_types{
        {"user_name", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>())},
        {"role_name", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>())},
        {"access_type", std::make_shared<DataTypeEnum16>(StorageSystemPrivileges::getAccessTypeEnumValues())},
        {"database", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>())},
        {"table", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>())},
        {"column", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>())},
        {"is_partial_revoke", std::make_shared<DataTypeUInt8>()},
        {"grant_option", std::make_shared<DataTypeUInt8>()},
    };
    return names_and_types;
}


void StorageSystemGrants::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const
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
    auto & column_access_type = assert_cast<ColumnInt16 &>(*res_columns[column_index++]).getData();
    auto & column_database = assert_cast<ColumnString &>(assert_cast<ColumnNullable &>(*res_columns[column_index]).getNestedColumn());
    auto & column_database_null_map = assert_cast<ColumnNullable &>(*res_columns[column_index++]).getNullMapData();
    auto & column_table = assert_cast<ColumnString &>(assert_cast<ColumnNullable &>(*res_columns[column_index]).getNestedColumn());
    auto & column_table_null_map = assert_cast<ColumnNullable &>(*res_columns[column_index++]).getNullMapData();
    auto & column_column = assert_cast<ColumnString &>(assert_cast<ColumnNullable &>(*res_columns[column_index]).getNestedColumn());
    auto & column_column_null_map = assert_cast<ColumnNullable &>(*res_columns[column_index++]).getNullMapData();
    auto & column_is_partial_revoke = assert_cast<ColumnUInt8 &>(*res_columns[column_index++]).getData();
    auto & column_grant_option = assert_cast<ColumnUInt8 &>(*res_columns[column_index++]).getData();

    auto add_row = [&](const String & grantee_name,
                       AccessEntityType grantee_type,
                       AccessType access_type,
                       const String * database,
                       const String * table,
                       const String * column,
                       bool is_partial_revoke,
                       bool grant_option)
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

        column_access_type.push_back(static_cast<Int16>(access_type));

        if (database)
        {
            column_database.insertData(database->data(), database->length());
            column_database_null_map.push_back(false);
        }
        else
        {
            column_database.insertDefault();
            column_database_null_map.push_back(true);
        }

        if (table)
        {
            column_table.insertData(table->data(), table->length());
            column_table_null_map.push_back(false);
        }
        else
        {
            column_table.insertDefault();
            column_table_null_map.push_back(true);
        }

        if (column)
        {
            column_column.insertData(column->data(), column->length());
            column_column_null_map.push_back(false);
        }
        else
        {
            column_column.insertDefault();
            column_column_null_map.push_back(true);
        }

        column_is_partial_revoke.push_back(is_partial_revoke);
        column_grant_option.push_back(grant_option);
    };

    auto add_rows = [&](const String & grantee_name,
                        AccessEntityType grantee_type,
                        const AccessRightsElements & elements)
    {
        for (const auto & element : elements)
        {
            auto access_types = element.access_flags.toAccessTypes();
            if (access_types.empty() || (!element.any_column && element.columns.empty()))
                continue;

            const auto * database = element.any_database ? nullptr : &element.database;
            const auto * table = element.any_table ? nullptr : &element.table;

            if (element.any_column)
            {
                for (const auto & access_type : access_types)
                    add_row(grantee_name, grantee_type, access_type, database, table, nullptr, element.is_partial_revoke, element.grant_option);
            }
            else
            {
                for (const auto & access_type : access_types)
                    for (const auto & column : element.columns)
                        add_row(grantee_name, grantee_type, access_type, database, table, &column, element.is_partial_revoke, element.grant_option);
            }
        }
    };

    for (const auto & id : ids)
    {
        auto entity = access_control.tryRead(id);
        if (!entity)
            continue;

        const AccessRights * access = nullptr;
        if (auto role = typeid_cast<RolePtr>(entity))
            access = &role->access;
        else if (auto user = typeid_cast<UserPtr>(entity))
            access = &user->access;
        else
            continue;

        const String & grantee_name = entity->getName();
        const auto grantee_type = entity->getType();
        auto elements = access->getElements();
        add_rows(grantee_name, grantee_type, elements);
    }
}

}
