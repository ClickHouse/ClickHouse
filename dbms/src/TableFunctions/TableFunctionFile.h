#pragma once

#include <TableFunctions/ITableFunction.h>


namespace DB
{

/* file(path, format, structure, useStorageMemory) - creates a temporary storage from file
 *
 *
 * The file must be in the current database data directory.
 * The relative path begins with the current database data directory.
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
