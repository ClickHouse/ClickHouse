#pragma once

#include <TableFunctions/ITableFunction.h>


namespace DB
{

/* file(path, format, structure) - creates a temporary storage from file
 *
 *
 * The file must be in the clickhouse data directory.
 * The relative path begins with the clickhouse data directory.
 */
    class TableFunctionFile : public ITableFunction
    {
    public:
        static constexpr auto name = "file";
        std::string getName() const override { return name; }
    private:
        StoragePtr executeImpl(const ASTPtr & ast_function, const Context & context) const override;
    };


}
