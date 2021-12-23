#pragma once

#include <TableFunctions/ITableFunction.h>

namespace DB
{

class Context;

/* executable(script_name_optional_arguments, format, structure, input_query) - creates a temporary storage from executable file
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
    StoragePtr executeImpl(const ASTPtr & ast_function, ContextPtr context, const std::string & table_name, ColumnsDescription cached_columns) const override;
    const char * getStorageTypeName() const override { return "Executable"; }

    ColumnsDescription getActualTableStructure(ContextPtr context) const override;
    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;

    String script_name;
    std::vector<String> arguments;
    String format;
    String structure;
    std::vector<ASTPtr> input_queries;
};
}
