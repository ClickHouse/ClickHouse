#include <Storages/System/StorageSystemGrants.h>
#include <Storages/System/SystemTableSourceRegistry.h>
#include <Storages/System/StorageSystemPrivileges.h>
#include <Storages/VirtualColumnUtils.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnSet.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Access/AccessControl.h>
#include <Access/Common/AccessRightsElement.h>
#include <Access/Role.h>
#include <Access/User.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <Interpreters/PreparedSets.h>
#include <boost/range/algorithm_ext/push_back.hpp>
#include <IO/WriteBufferFromString.h>
#include <algorithm>

namespace DB
{
namespace
{

const ActionsDAG::Node & skipAliases(const ActionsDAG::Node & node)
{
    if (node.type == ActionsDAG::ActionType::ALIAS)
        return skipAliases(*node.children.at(0));
    return node;
}

bool isGranteeName(const ActionsDAG::Node & node)
{
    return node.type == ActionsDAG::ActionType::INPUT
        && (node.result_name == "user_name" || node.result_name == "role_name");
}

bool isStringConstant(const ActionsDAG::Node & node)
{
    return node.type == ActionsDAG::ActionType::COLUMN && isString(removeNullable(node.result_type));
}

bool canFilterGrantees(const ActionsDAG::Node & predicate)
{
    const auto & node = skipAliases(predicate);
    if (node.type != ActionsDAG::ActionType::FUNCTION)
        return false;

    const auto & name = node.function_base->getName();
    if (name == "and" || name == "or")
        return std::ranges::all_of(node.children, [](const auto * child) { return canFilterGrantees(*child); });

    if (node.children.size() != 2)
        return false;

    const auto & lhs = skipAliases(*node.children[0]);
    const auto & rhs = skipAliases(*node.children[1]);
    if (name == "equals")
        return (isGranteeName(lhs) && isStringConstant(rhs)) || (isGranteeName(rhs) && isStringConstant(lhs));
    if (name == "in")
    {
        if (!isGranteeName(lhs) || rhs.type != ActionsDAG::ActionType::COLUMN || !rhs.column)
            return false;

        const auto * column_set = checkAndGetColumn<ColumnSet>(&rhs.column->getDataColumn());
        /// A concurrent insert into a `Set` table can change the final predicate after this
        /// prefilter has discarded an entity. Only prune with sets that are fixed for the query.
        return column_set && column_set->getData() && !column_set->getData()->isMutableDuringQuery();
    }
    return false;
}

}

ColumnsDescription StorageSystemGrants::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"user_name", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "User name."},
        {"role_name", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "Role assigned to user account."},
        {"access_type", std::make_shared<DataTypeEnum16>(StorageSystemPrivileges::getAccessTypeEnumValues()), "Access parameters for ClickHouse user account."},
        {"access_object", std::make_shared<DataTypeString>(), "Parameter for access_type. Contains: "
            "1 - Name of the source type for READ/WRITE grants. "
            "2 - Name of the table engine for TABLE ENGINE grants. "
            "3 - Name of the user for grants like `SET DEFINER`."
        },
        {"database", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "Name of a database."},
        {"table", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "Name of a table."},
        {"column", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "Name of a column to which access is granted."},
        {"is_partial_revoke", std::make_shared<DataTypeUInt8>(),
            "Logical value. It shows whether some privileges have been revoked. Possible values: "
            "0 — The row describes a grant, "
            "1 — The row describes a partial revoke."
        },
        {"grant_option", std::make_shared<DataTypeUInt8>(), "Permission is granted WITH GRANT OPTION."},
        {"is_wildcard", std::make_shared<DataTypeUInt8>(),
            "Logical value. It shows whether the grant is a wildcard prefix grant. Possible values: "
            "0 — The row does not describe a wildcard prefix grant, "
            "1 — The row describes a wildcard prefix grant (e.g. db*.* or foo.bar*)."
        },
        {"is_obsolete", std::make_shared<DataTypeUInt8>(),
            "Whether the granted privilege is obsolete. Such grants have no effect anymore and can be revoked."
        },
    };
}


Block StorageSystemGrants::getFilterSampleBlock() const
{
    auto name_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
    return {{{}, name_type, "user_name"}, {{}, name_type, "role_name"}};
}


void StorageSystemGrants::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node * predicate, std::vector<UInt8>) const
{
    /// If "select_from_system_db_requires_grant" is enabled the access rights were already checked in InterpreterSelectQuery.
    const auto & access_control = context->getAccessControl();
    if (!access_control.doesSelectFromSystemDatabaseRequireGrant())
        context->checkAccess(AccessType::SHOW_USERS | AccessType::SHOW_ROLES);

    bool is_enabled_read_write_grants = access_control.isEnabledReadWriteGrants();

    std::vector<UUID> ids = access_control.findAll<User>();
    boost::range::push_back(ids, access_control.findAll<Role>());

    /// The base class extracts predicates on grantee names. Restrict early evaluation to simple
    /// name comparisons because entities with no grants produce no rows on which a general
    /// expression could otherwise throw. Keep the expression intact when filtering the names.
    if (predicate && canFilterGrantees(*predicate))
    {
        Block grantees = getFilterSampleBlock();
        auto names = grantees.cloneEmptyColumns();
        auto entity_ids = ColumnUUID::create();
        for (const auto & id : ids)
        {
            const auto grantee = access_control.tryReadNameWithType(id);
            if (!grantee)
                continue;

            const auto & [name, type] = *grantee;
            names[0]->insert(type == AccessEntityType::USER ? Field{name} : Field{});
            names[1]->insert(type == AccessEntityType::ROLE ? Field{name} : Field{});
            entity_ids->insertValue(id);
        }
        grantees.setColumns(std::move(names));
        grantees.insert({std::move(entity_ids), std::make_shared<DataTypeUUID>(), "_entity_id"});
        VirtualColumnUtils::filterBlockWithPredicate(predicate, grantees, context);
        const auto & matched = assert_cast<const ColumnUUID &>(*grantees.getByName("_entity_id").column).getData();
        ids.assign(matched.begin(), matched.end());
    }

    size_t column_index = 0;
    auto & column_user_name = assert_cast<ColumnString &>(assert_cast<ColumnNullable &>(*res_columns[column_index]).getNestedColumn());
    auto & column_user_name_null_map = assert_cast<ColumnNullable &>(*res_columns[column_index++]).getNullMapData();
    auto & column_role_name = assert_cast<ColumnString &>(assert_cast<ColumnNullable &>(*res_columns[column_index]).getNestedColumn());
    auto & column_role_name_null_map = assert_cast<ColumnNullable &>(*res_columns[column_index++]).getNullMapData();
    auto & column_access_type = assert_cast<ColumnInt16 &>(*res_columns[column_index++]).getData();
    auto & column_access_object = assert_cast<ColumnString &>(*res_columns[column_index++]);
    auto & column_database = assert_cast<ColumnString &>(assert_cast<ColumnNullable &>(*res_columns[column_index]).getNestedColumn());
    auto & column_database_null_map = assert_cast<ColumnNullable &>(*res_columns[column_index++]).getNullMapData();
    auto & column_table = assert_cast<ColumnString &>(assert_cast<ColumnNullable &>(*res_columns[column_index]).getNestedColumn());
    auto & column_table_null_map = assert_cast<ColumnNullable &>(*res_columns[column_index++]).getNullMapData();
    auto & column_column = assert_cast<ColumnString &>(assert_cast<ColumnNullable &>(*res_columns[column_index]).getNestedColumn());
    auto & column_column_null_map = assert_cast<ColumnNullable &>(*res_columns[column_index++]).getNullMapData();
    auto & column_is_partial_revoke = assert_cast<ColumnUInt8 &>(*res_columns[column_index++]).getData();
    auto & column_grant_option = assert_cast<ColumnUInt8 &>(*res_columns[column_index++]).getData();
    auto & column_is_wildcard = assert_cast<ColumnUInt8 &>(*res_columns[column_index++]).getData();
    auto & column_is_obsolete = assert_cast<ColumnUInt8 &>(*res_columns[column_index++]).getData();

    auto add_row = [&](const String & grantee_name,
                       AccessEntityType grantee_type,
                       AccessType access_type,
                       const String & access_object,
                       const String * database,
                       const String * table,
                       const String * column,
                       bool is_partial_revoke,
                       bool grant_option,
                       bool is_wildcard)
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
            chassert(false);

        column_access_type.push_back(static_cast<Int16>(access_type));
        column_access_object.insertData(access_object.data(), access_object.length());

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
        column_is_wildcard.push_back(is_wildcard);
        column_is_obsolete.push_back(isObsolete(access_type));
    };

    auto add_rows = [&](const String & grantee_name,
                        AccessEntityType grantee_type,
                        const AccessRightsElements & elements)
    {
        for (const auto & element : elements)
        {
            auto access_types = element.access_flags.toAccessTypes();
            if (access_types.empty() || (!element.anyColumn() && element.columns.empty()))
                continue;

            const auto * database = element.anyDatabase() ? nullptr : &element.database;
            const auto * table = element.anyTable() ? nullptr : &element.table;

            String access_object = element.parameter;
            if (element.hasFilter() && is_enabled_read_write_grants)
            {
                WriteBufferFromOwnString buf;
                element.formatFilter(buf);
                access_object += buf.str();
            }

            const bool is_wildcard = element.wildcard;

            if (element.anyColumn())
            {
                for (const auto & access_type : access_types)
                    add_row(grantee_name, grantee_type, access_type, access_object, database, table, nullptr, element.is_partial_revoke, element.grant_option, is_wildcard);
            }
            else
            {
                for (const auto & access_type : access_types)
                    for (const auto & column : element.columns)
                        add_row(grantee_name, grantee_type, access_type, access_object, database, table, &column, element.is_partial_revoke, element.grant_option, is_wildcard);
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

/// Register the source file of this system table for `system.documentation`.
namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemGrants) }
