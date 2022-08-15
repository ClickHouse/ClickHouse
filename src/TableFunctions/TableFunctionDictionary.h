#pragma once

#include <TableFunctions/ITableFunction.h>

namespace DB
{
class Context;

/* file(path, format, structure) - creates a temporary storage from file
 *
 * The file must be in the clickhouse data directory.
 * The relative path begins with the clickhouse data directory.
 */
class TableFunctionDictionary final : public ITableFunction
{
public:
    static constexpr auto name = "dictionary";
    std::string getName() const override
    {
        return name;
    }

    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;

    ColumnsDescription getActualTableStructure(ContextPtr context) const override;

    StoragePtr executeImpl(const ASTPtr & ast_function, ContextPtr context, const std::string & table_name, ColumnsDescription) const override;

    const char * getStorageTypeName() const override { return "Dictionary"; }

private:
    String dictionary_name;
    ColumnsDescription dictionary_columns;
};}
