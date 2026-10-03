#include <Storages/System/StorageSystemUserDefinedTypes.h>

#include <Access/AccessControl.h>
#include <Access/Common/AccessType.h>
#include <Access/ContextAccess.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/UserDefinedTypeFactory.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateTypeQuery.h>
#include <Storages/System/SystemTableSourceRegistry.h>

namespace DB
{

ColumnsDescription StorageSystemUserDefinedTypes::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"name", std::make_shared<DataTypeString>(), "Name of the user-defined type."},
        {"base_type", std::make_shared<DataTypeString>(), "The data type expression the user-defined type expands to."},
        {"type_parameters", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "The formal parameters of a parameterized type, or NULL if the type has none."},
        {"create_query", std::make_shared<DataTypeString>(), "The CREATE TYPE query defining the type."},
    };
}

void StorageSystemUserDefinedTypes::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    /// `SHOW TYPES` / `SHOW TYPE` are gated by this privilege, and this table exposes the same information.
    /// If `select_from_system_db_requires_grant` is enabled the access rights were already checked in InterpreterSelectQuery.
    if (!context->getAccessControl().doesSelectFromSystemDatabaseRequireGrant())
        context->checkAccess(AccessType::SHOW_USER_DEFINED_TYPES);

    const auto & udt_factory = UserDefinedTypeFactory::instance();
    for (const auto & type_name : udt_factory.getAllRegisteredNames())
    {
        /// The type may have been dropped concurrently.
        auto create_query = udt_factory.tryGet(type_name);
        if (!create_query)
            continue;
        const auto & create = create_query->as<const ASTCreateTypeQuery &>();

        res_columns[0]->insert(type_name);
        res_columns[1]->insert(create.base_type->formatWithSecretsOneLine());
        if (create.type_parameters)
            res_columns[2]->insert(create.type_parameters->formatWithSecretsOneLine());
        else
            res_columns[2]->insertDefault();
        res_columns[3]->insert(create_query->formatWithSecretsOneLine());
    }
}

}

/// Register the source file of this system table for `system.documentation`.
namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemUserDefinedTypes) }
