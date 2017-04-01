#pragma once

#include <Parsers/IAST.h>
#include <Storages/IStorage.h>
#include <vector>


namespace DB
{

class Context;
struct CollectAliases;
class WriteBuffer;


/** Collect and analyze table-like expressions in section FROM in a query.
  * For every expression, keep its alias.
  *
  * For ordinary tables, determine database and table name, obtain and keep StoragePtr.
  * For subqueries, determine result structure. This requires analysis of subquery, such as type inference.
  * For table functions, execute them to obtain resulting StoragePtr.
  *
  * NOTE: We assume, that execution of table functions is cheap, as we do it during analysis.
  */
struct CollectTables
{
    void process(ASTPtr & ast, Context & context, const CollectAliases & aliases);

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
