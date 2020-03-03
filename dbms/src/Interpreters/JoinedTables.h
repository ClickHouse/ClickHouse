#pragma once

#include <Core/NamesAndTypes.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Storages/IStorage_fwd.h>

namespace DB
{

class ASTSelectQuery;
class Context;

/// Joined tables' columns resolver.
/// We want to get each table structure at most once per table occurance. Or even better once per table.
/// TODO: joins tree with costs to change joins order by CBO.
class JoinedTables
{
public:
    JoinedTables() = default;
    JoinedTables(const ASTSelectQuery & select_query);

    void reset(const ASTSelectQuery & select_query)
    {
        *this = JoinedTables(select_query);
    }

    StoragePtr getLeftTableStorage(Context & context);

    /// Resolve columns or get from storage. It assumes storage is not nullptr.
    void resolveTables(const Context & context, StoragePtr storage);
    /// Resolve columns or get from source list.
    void resolveTables(const Context & context, const NamesAndTypesList & source_columns);

    const std::vector<TableWithColumnNamesAndTypes> & tablesWithColumns() const { return tables_with_columns; }
    const NamesAndTypesList & secondTableColumns() const;

    bool isLeftTableSubquery() const;
    bool isLeftTableFunction() const;
    bool hasJoins() const { return table_expressions.size() > 1; }

    const ASTPtr & leftTableExpression() const { return left_table_expression; }
    const String & leftTableDatabase() const { return database_name; }
    const String & leftTableName() const { return table_name; }

private:
    std::vector<const ASTTableExpression *> table_expressions;
    std::vector<TableWithColumnNamesAndTypes> tables_with_columns;

    /// Legacy (duplicated left table values)
    ASTPtr left_table_expression;
    std::optional<DatabaseAndTableWithAlias> left_db_and_table;

    /// left_db_and_table or 'system.one'
    String database_name;
    String table_name;
};

}
