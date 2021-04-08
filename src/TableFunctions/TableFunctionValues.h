#pragma once

#include <TableFunctions/ITableFunction.h>

namespace DB
{
/* values(structure, values...) - creates a temporary storage filling columns with values
 * values is case-insensitive table function
 */
class TableFunctionValues : public ITableFunction
{
public:
    static constexpr auto name = "values";
    std::string getName() const override { return name; }
private:
    StoragePtr executeImpl(const ASTPtr & ast_function, const Context & context, const std::string & table_name) const override;
    const char * getStorageTypeName() const override { return "Values"; }
};


}
