#pragma once

#include <TableFunctions/ITableFunction.h>


namespace DB
{

/* file(path, format, structure) - creates a temporary StorageMemory from file
 * The file must be in the data directory on clickhouse server.
 * The relative path begins with the data directory on clickhouse server.
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
