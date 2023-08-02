#pragma once

#include <TableFunctions/ITableFunction.h>
#include <Parsers/ASTCreateQuery.h>
#include <base/types.h>

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

    const ASTSelectWithUnionQuery & getSelectQuery() const;

private:
    StoragePtr executeImpl(const ASTPtr & ast_function, ContextPtr context, const String & table_name, ColumnsDescription cached_columns) const override;
    const char * getStorageTypeName() const override { return "View"; }

    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;
    ColumnsDescription getActualTableStructure(ContextPtr context) const override;

    ASTCreateQuery create;
};


}
