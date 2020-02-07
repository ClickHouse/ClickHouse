#pragma once

#include <TableFunctions/ITableFunction.h>

namespace DB
{
/* random(structure, limit) - creates a temporary storage filling columns with random data
 * random is case-insensitive table function
 */
class TableFunctionRandom : public ITableFunction
{
public:
    static constexpr auto name = "generate";
    std::string getName() const override { return name; }
private:
    StoragePtr executeImpl(const ASTPtr & ast_function, const Context & context, const std::string & table_name) const override;
};


}
