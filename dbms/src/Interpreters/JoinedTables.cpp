#include <Interpreters/JoinedTables.h>
#include <Interpreters/Context.h>
#include <Interpreters/getTableExpressions.h>
#include <Storages/IStorage.h>
#include <Storages/ColumnsDescription.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ALIAS_REQUIRED;
}

namespace
{

std::vector<TableWithColumnNames> getTablesWithColumns(const std::vector<const ASTTableExpression * > & table_expressions,
                                                const Context & context)
{
    std::vector<TableWithColumnNames> tables_with_columns = getDatabaseAndTablesWithColumnNames(table_expressions, context);

    auto & settings = context.getSettingsRef();
    if (settings.joined_subquery_requires_alias && tables_with_columns.size() > 1)
    {
        for (auto & pr : tables_with_columns)
            if (pr.table.table.empty() && pr.table.alias.empty())
                throw Exception("No alias for subquery or table function in JOIN (set joined_subquery_requires_alias=0 to disable restriction).",
                                ErrorCodes::ALIAS_REQUIRED);
    }

    return tables_with_columns;
}

}


void JoinedTables::resolveTables(const ASTSelectQuery & select_query, StoragePtr storage, const Context & context,
                                 const NamesAndTypesList & source_columns)
{
    if (!storage)
    {
        if (auto db_and_table = getDatabaseAndTable(select_query, 0))
            storage = context.tryGetTable(db_and_table->database, db_and_table->table);
    }

    std::vector<const ASTTableExpression *> table_expressions = getTableExpressions(select_query);
    tables_with_columns = getTablesWithColumns(table_expressions, context);

    if (tables_with_columns.empty())
    {
        if (storage)
        {
            const ColumnsDescription & storage_columns = storage->getColumns();
            tables_with_columns.emplace_back(DatabaseAndTableWithAlias{}, storage_columns.getOrdinary().getNames());
            auto & table = tables_with_columns.back();
            table.addHiddenColumns(storage_columns.getMaterialized());
            table.addHiddenColumns(storage_columns.getAliases());
            table.addHiddenColumns(storage_columns.getVirtuals());
        }
        else
        {
            Names columns;
            columns.reserve(source_columns.size());
            for (const auto & column : source_columns)
                columns.push_back(column.name);
            tables_with_columns.emplace_back(DatabaseAndTableWithAlias{}, columns);
        }
    }

    if (table_expressions.size() > 1)
        columns_from_joined_table = getColumnsFromTableExpression(*table_expressions[1], context);
}

}
