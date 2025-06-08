#include <Access/AccessRights.h>
#include <Access/ContextAccess.h>
#include <Access/AccessControl.h>
#include <Access/EnabledRolesInfo.h>
#include <Access/Role.h>
#include <Access/User.h>
#include <Columns/ColumnString.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeString.h>
#include <Interpreters/Context.h>
#include <Poco/Net/IPAddress.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/StorageValues.h>
#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Parsers/IAST.h>
#include "registerTableFunctions.h"


namespace DB
{

namespace
{

class TableFunctionCurrentGrants : public ITableFunction
{
public:
    static constexpr auto name = "currentGrants";
    std::string getName() const override { return name; }

private:
    StoragePtr executeImpl(const ASTPtr & ast_function, ContextPtr context, const String & table_name, ColumnsDescription cached_columns, bool is_insert_query) const override;
    ColumnsDescription getActualTableStructure(ContextPtr context, bool is_insert_query) const override;
    const char * getStorageTypeName() const override { return "Values"; }
    void collectGrantBySource(const String & source_name, const AccessRights & access_rights) const;

    mutable IColumn::MutablePtr grant_column;
    mutable IColumn::MutablePtr source_column;
};


ColumnsDescription TableFunctionCurrentGrants::getActualTableStructure(ContextPtr /*context*/, bool /*is_insert_query*/) const
{
    return ColumnsDescription{
        { "grant", std::make_shared<DataTypeString>() },
        { "source", std::make_shared<DataTypeString>() }
    };
}

void TableFunctionCurrentGrants::collectGrantBySource(const String & source_name, const AccessRights & access_rights) const
{
    for (const auto & element : access_rights.getElements())
    {
        for (const auto & type : element.access_flags.toAccessTypes())
        {
            AccessRightsElement single_grant{type};
            single_grant.database = element.database;
            single_grant.table = element.table;
            single_grant.columns = element.columns;
            single_grant.grant_option = element.grant_option;
            grant_column->insert(single_grant.toString());
            source_column->insert(source_name);
        }
    }
}

StoragePtr TableFunctionCurrentGrants::executeImpl(const ASTPtr & /*ast_function*/, ContextPtr context, const String & table_name, ColumnsDescription /*cached_columns*/, bool /*is_insert_query*/) const
{
    auto access = context->getAccess();
    const auto & access_control = context->getAccessControl();

    grant_column = ColumnString::create();
    source_column = ColumnString::create();

    // Grants from user
    if (auto user = access->tryGetUser())
        collectGrantBySource(user->getName(), user->access);

    // Grants from roles
    if (auto roles_info = access->getRolesInfo())
    {
        for (const auto & role_id : roles_info->enabled_roles)
        {
            auto role = access_control.tryRead<Role>(role_id);
            if (role)
                collectGrantBySource(role->getName(), role->access);
        }
    }

    Block res_block{
        { std::move(grant_column), std::make_shared<DataTypeString>(), "grant" },
        { std::move(source_column), std::make_shared<DataTypeString>(), "source" }
    };

    auto res = std::make_shared<StorageValues>(StorageID(getDatabaseName(), table_name), getActualTableStructure(context, false), res_block);
    res->startup();
    return res;
}

}

void registerTableFunctionCurrentGrants(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionCurrentGrants>({.documentation = {}, .allow_readonly = true});
    factory.registerAlias("current_grants", "currentGrants", TableFunctionFactory::Case::Insensitive);
}

}
