#pragma once

#include <Parsers/IAST.h>
#include <Storages/IStorage.h>
#include <vector>


namespace DB
{

class Context;
struct CollectAliases;
struct ExecuteTableFunctions;
class WriteBuffer;


/** Collect and analyze table-like expressions in section FROM in a query.
  * For every expression, keep its alias.
  *
  * For ordinary tables, determine database and table name, obtain and keep StoragePtr.
  * For subqueries, determine result structure. This requires analysis of subquery, such as type inference.
  * For table functions, grab them from prepared ExecuteTableFunctions object.
  */
struct CollectTables
{
    void process(ASTPtr & ast, const Context & context, const CollectAliases & aliases, ExecuteTableFunctions & table_functions);

    enum class Kind
    {
        OrdinaryTable,
        TableFunction,
        Subquery
    };

    struct TableInfo
    {
        ASTPtr node;
        String database_name;
        String table_name;
        String alias;
        StoragePtr storage;
        Block structure_of_subquery;
    };

    using Tables = std::vector<TableInfo>;
    Tables tables;

    /// Debug output
    void dump(WriteBuffer & out) const;
};

}
