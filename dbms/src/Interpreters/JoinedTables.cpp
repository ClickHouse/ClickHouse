#include <Interpreters/JoinedTables.h>
#include <Interpreters/Context.h>
#include <Interpreters/getTableExpressions.h>
#include <Storages/IStorage.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/StorageValues.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectWithUnionQuery.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ALIAS_REQUIRED;
}

namespace
{

template <typename T>
void checkTablesWithColumns(const std::vector<T> & tables_with_columns, const Context & context)
{
    auto & settings = context.getSettingsRef();
    if (settings.joined_subquery_requires_alias && tables_with_columns.size() > 1)
    {
        for (auto & t : tables_with_columns)
            if (t.table.table.empty() && t.table.alias.empty())
                throw Exception("No alias for subquery or table function in JOIN (set joined_subquery_requires_alias=0 to disable restriction).",
                                ErrorCodes::ALIAS_REQUIRED);
    }
}

}

JoinedTables::JoinedTables(const ASTSelectQuery & select_query)
    : table_expressions(getTableExpressions(select_query))
    , left_table_expression(extractTableExpression(select_query, 0))
    , left_db_and_table(getDatabaseAndTable(select_query, 0))
{}

bool JoinedTables::isLeftTableSubquery() const
{
    return left_table_expression && left_table_expression->as<ASTSelectWithUnionQuery>();
}

bool JoinedTables::isLeftTableFunction() const
{
    return left_table_expression && left_table_expression->as<ASTFunction>();
}

StoragePtr JoinedTables::getLeftTableStorage(Context & context)
{
    StoragePtr storage;

    if (left_db_and_table)
    {
        database_name = left_db_and_table->database;
        table_name = left_db_and_table->table;

        /// If the database is not specified - use the current database.
        if (database_name.empty() && !context.isExternalTableExist(table_name))
            database_name = context.getCurrentDatabase();
    }
    else /// If the table is not specified - use the table `system.one`.
    {
        database_name = "system";
        table_name = "one";
    }

    if (auto view_source = context.getViewSource())
    {
        auto & storage_values = static_cast<const StorageValues &>(*view_source);
        auto tmp_table_id = storage_values.getStorageID();
        if (tmp_table_id.database_name == database_name && tmp_table_id.table_name == table_name)
        {
            /// Read from view source.
            storage = context.getViewSource();
        }
    }

    if (!storage)
    {
        /// Read from table. Even without table expression (implicit SELECT ... FROM system.one).
        storage = context.getTable(database_name, table_name);
    }

    return storage;
}

const NamesAndTypesList & JoinedTables::secondTableColumns() const
{
    static const NamesAndTypesList empty;
    if (tables_with_columns.size() > 1)
        return tables_with_columns[1].columns;
    return empty;
}

void JoinedTables::resolveTables(const Context & context, StoragePtr storage)
{
    tables_with_columns = getDatabaseAndTablesWithColumns(table_expressions, context);
    checkTablesWithColumns(tables_with_columns, context);

    if (tables_with_columns.empty())
    {
        const ColumnsDescription & storage_columns = storage->getColumns();
        tables_with_columns.emplace_back(DatabaseAndTableWithAlias{}, storage_columns.getOrdinary());
        auto & table = tables_with_columns.back();
        table.addHiddenColumns(storage_columns.getMaterialized());
        table.addHiddenColumns(storage_columns.getAliases());
        table.addHiddenColumns(storage_columns.getVirtuals());
    }
}

void JoinedTables::resolveTables(const Context & context, const NamesAndTypesList & source_columns)
{
    tables_with_columns = getDatabaseAndTablesWithColumns(table_expressions, context);
    checkTablesWithColumns(tables_with_columns, context);

    if (tables_with_columns.empty())
        tables_with_columns.emplace_back(DatabaseAndTableWithAlias{}, source_columns);
}

}
