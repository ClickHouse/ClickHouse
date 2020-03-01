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
    void resolveTables(const ASTSelectQuery & select_query, StoragePtr storage, const Context & context,
                       const NamesAndTypesList & source_columns);

    const std::vector<TableWithColumnNames> & tablesWithColumns() const { return tables_with_columns; }
    const NamesAndTypesList & secondTableColumns() const { return columns_from_joined_table; }

private:
    std::vector<TableWithColumnNames> tables_with_columns;
    NamesAndTypesList columns_from_joined_table;
};

}
