#pragma once

#include <TableFunctions/ITableFunction.h>
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
    StoragePtr executeImpl(const ASTPtr & ast_function, const Context & context, const std::string & table_name) const override;
    const char * getStorageTypeName() const override { return "View"; }

    UInt64 evaluateArgument(const Context & context, ASTPtr & argument) const;
};


}
