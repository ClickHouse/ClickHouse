#pragma once

#include <DataStreams/IBlockStream_fwd.h>
#include <TableFunctions/ITableFunction.h>

namespace DB
{

class Context;

/* executable(path, format, structure, input_query) - creates a temporary storage from executable file
 *
 *
 * The file must be in the clickhouse data directory.
 * The relative path begins with the clickhouse data directory.
 */
class TableFunctionExecutable : public ITableFunction
{
public:
    static constexpr auto name = "executable";
    std::string getName() const override { return name; }
    bool hasStaticStructure() const override { return true; }

private:
    StoragePtr executeImpl(const ASTPtr & ast_function, const Context & context, const std::string & table_name, ColumnsDescription cached_columns) const override;
    const char * getStorageTypeName() const override { return "Executable"; }

    ColumnsDescription getActualTableStructure(const Context & context) const override;
    void parseArguments(const ASTPtr & ast_function, const Context & context) override;

    String file_path;
    String format;
    String structure;
    BlockInputStreamPtr input;
};
}
