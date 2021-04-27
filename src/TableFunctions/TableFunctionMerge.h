#pragma once

#include <TableFunctions/ITableFunction.h>


namespace DB
{

/* merge (db_name, tables_regexp) - creates a temporary StorageMerge.
 * The structure of the table is taken from the first table that came up, suitable for regexp.
 * If there is no such table, an exception is thrown.
 */
class TableFunctionMerge : public ITableFunction
{
public:
    static constexpr auto name = "merge";
    std::string getName() const override { return name; }
private:
    StoragePtr executeImpl(const ASTPtr & ast_function, ContextPtr context, const std::string & table_name, ColumnsDescription cached_columns) const override;
    const char * getStorageTypeName() const override { return "Merge"; }

    const Strings & getSourceTables(ContextPtr context) const;
    ColumnsDescription getActualTableStructure(ContextPtr context) const override;
    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;

    String source_database;
    String source_table_regexp;
    mutable std::optional<Strings> source_tables;
};


}
