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
    StoragePtr execute(const ASTPtr & ast_function, const Context & context) const override;
};


}
