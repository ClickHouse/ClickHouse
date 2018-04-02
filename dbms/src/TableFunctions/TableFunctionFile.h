#pragma once

#include <TableFunctions/ITableFunction.h>


namespace DB
{

/* file(path, format, structure)
 * Creates a temporary StorageMemory from file
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
