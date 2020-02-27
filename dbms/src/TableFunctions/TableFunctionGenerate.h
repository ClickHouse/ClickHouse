#pragma once

#include <TableFunctions/ITableFunction.h>

namespace DB
{
/* random(structure(, max_array_length, max_string_length, random_seed)) - creates a temporary storage that generates columns with random data
 */
class TableFunctionGenerate : public ITableFunction
{
public:
    static constexpr auto name = "generate";
    std::string getName() const override { return name; }
private:
    StoragePtr executeImpl(const ASTPtr & ast_function, const Context & context, const std::string & table_name) const override;
};


}
