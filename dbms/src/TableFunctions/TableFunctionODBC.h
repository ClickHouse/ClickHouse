#pragma once

#include <TableFunctions/ITableFunction.h>


namespace DB
{

/* odbc (database, table) - creates a temporary StorageODBC.
 */
class TableFunctionODBC : public ITableFunction
{
public:
    static constexpr auto name = "odbc";
    std::string getName() const override { return name; }
    StoragePtr execute(const ASTPtr & ast_function, const Context & context) const override;
};


}
