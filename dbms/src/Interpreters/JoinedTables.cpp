#include <Interpreters/JoinedTables.h>
#include <Interpreters/Context.h>
#include <Interpreters/getTableExpressions.h>
#include <Storages/IStorage.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/StorageValues.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>

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

JoinedTables::JoinedTables(Context && context_, const ASTSelectQuery & select_query)
    : context(context_)
    , table_expressions(getTableExpressions(select_query))
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

std::unique_ptr<InterpreterSelectWithUnionQuery> JoinedTables::makeLeftTableSubquery(const SelectQueryOptions & select_options)
{
    if (!isLeftTableSubquery())
        return {};
    return std::make_unique<InterpreterSelectWithUnionQuery>(left_table_expression, context, select_options);
}

StoragePtr JoinedTables::getLeftTableStorage()
{
    if (isLeftTableSubquery())
        return {};

    if (isLeftTableFunction())
        return context.executeTableFunction(left_table_expression);

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
            return context.getViewSource();
        }
    }

    /// Read from table. Even without table expression (implicit SELECT ... FROM system.one).
    return context.getTable(database_name, table_name);
}

bool JoinedTables::resolveTables()
{
    tables_with_columns = getDatabaseAndTablesWithColumns(table_expressions, context);
    checkTablesWithColumns(tables_with_columns, context);

    return !tables_with_columns.empty();
}

void JoinedTables::makeFakeTable(StoragePtr storage, const Block & source_header)
{
    if (storage)
    {
        const ColumnsDescription & storage_columns = storage->getColumns();
        tables_with_columns.emplace_back(DatabaseAndTableWithAlias{}, storage_columns.getOrdinary());

        auto & table = tables_with_columns.back();
        table.addHiddenColumns(storage_columns.getMaterialized());
        table.addHiddenColumns(storage_columns.getAliases());
        table.addHiddenColumns(storage_columns.getVirtuals());
    }
    else
        tables_with_columns.emplace_back(DatabaseAndTableWithAlias{}, source_header.getNamesAndTypesList());
}

}
