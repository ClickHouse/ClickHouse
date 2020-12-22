#pragma once

#include <TableFunctions/ITableFunction.h>
#include <Parsers/ASTCreateQuery.h>
#include <common/types.h>

namespace DB
{

/* view(query)
 * Turning subquery into a table.
 * Useful for passing subquery around.
 */
class TableFunctionView : public ITableFunction
{
public:
    static constexpr auto name = "view";
    std::string getName() const override { return name; }
private:
    StoragePtr executeImpl(const ASTPtr & ast_function, const Context & context, const String & table_name, ColumnsDescription cached_columns) const override;
    const char * getStorageTypeName() const override { return "View"; }

    void parseArguments(const ASTPtr & ast_function, const Context & context) override;
    ColumnsDescription getActualTableStructure(const Context & context) const override;

    ASTCreateQuery create;
};


}
