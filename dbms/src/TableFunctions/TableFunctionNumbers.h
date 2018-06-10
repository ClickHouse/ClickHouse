#pragma once

#include <TableFunctions/ITableFunction.h>


namespace DB
{

/* numbers(limit)
 * - the same as SELECT number FROM system.numbers LIMIT limit.
 * Used for testing purposes, as a simple example of table function.
 */
class TableFunctionNumbers : public ITableFunction
{
public:
    static constexpr auto name = "numbers";
    std::string getName() const override { return name; }
private:
    StoragePtr executeImpl(const ASTPtr & ast_function, const Context & context) const override;
};


}
