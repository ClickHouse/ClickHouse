#pragma once

#include <Common/config.h>
#include <TableFunctions/ITableFunction.h>


namespace DB
{
/* odbc (odbc connect string, table) - creates a temporary StorageODBC.
 */
class TableFunctionODBC : public ITableFunction
{
public:
    static constexpr auto name = "odbc";
    std::string getName() const override
    {
        return name;
    }
private:
    StoragePtr executeImpl(const ASTPtr & ast_function, const Context & context) const override;
};
}
