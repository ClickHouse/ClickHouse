#pragma once

#include <TableFunctions/ITableFunction.h>
#include <Core/Types.h>


namespace DB
{

/* null(structure) - creates a temporary null storage
 *
 * Used for testing purposes, for convenience writing tests and demos.
 */
class TableFunctionNull : public ITableFunction
{
public:
    static constexpr auto name = "null";
    std::string getName() const override { return name; }
private:
    StoragePtr executeImpl(const ASTPtr & ast_function, ContextPtr context, const String & table_name, ColumnsDescription cached_columns) const override;
    const char * getStorageTypeName() const override { return "Null"; }

    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;
    ColumnsDescription getActualTableStructure(ContextPtr context) const override;

    String structure;
};

}
