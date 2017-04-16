#pragma once

#include <TableFunctions/ITableFunction.h>


namespace DB
{

/*
 * merge (db_name, tables_regexp) - creates a temporary StorageMerge.
 * The structure of the table is taken from the first table that came up, suitable for regexp.
 * If there is no such table, an exception is thrown.
 */
class TableFunctionMerge: public ITableFunction
{
public:
    std::string getName() const override { return "merge"; }
    StoragePtr execute(ASTPtr ast_function, Context & context) const override;
};


}
