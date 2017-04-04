#pragma once

#include <Parsers/IAST.h>
#include <Analyzers/CollectTables.h>
#include <unordered_map>


namespace DB
{

class WriteBuffer;
struct CollectAliases;
struct CollectTables;


/** For every identifier, that is not an alias,
  *  determine from what table it comes,
  *  its original name in table,
  *  and its data type.
  *
  * Also:
  * - expand asterisks (such as *, t.*, db.table.* and (TODO) even db.table.nested.*) to corresponding list of columns;
  * - translate count(*) to count();
  * - TODO expand alias columns that come from table definition;
  * - TODO replace column names to fully qualified names: identical columns will have same names.
  *
  * If column is not found or in case of ambiguity, throw an exception.
  */
struct AnalyzeColumns
{
    void process(ASTPtr & ast, const CollectAliases & aliases, const CollectTables & tables);

    struct ColumnInfo
    {
        ASTPtr node;
        CollectTables::TableInfo table;
        String name_in_table;
        DataTypePtr data_type;
    };

    using Columns = std::unordered_map<String, ColumnInfo>;
    Columns columns;

    /// Debug output
    void dump(WriteBuffer & out) const;
};

}
