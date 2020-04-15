#pragma once

#include <TableFunctions/ITableFunction.h>
#include <Core/Types.h>


namespace DB
{

/* numbers(limit), numbers_mt(limit)
 * - the same as SELECT number FROM system.numbers LIMIT limit.
 * Used for testing purposes, as a simple example of table function.
 */
template <bool multithreaded>
class TableFunctionNumbers : public ITableFunction
{
public:
    static constexpr auto name = multithreaded ? "numbers_mt" : "numbers";
    std::string getName() const override { return name; }
private:
    StoragePtr executeImpl(const ASTPtr & ast_function, const Context & context, const std::string & table_name) const override;
    const char * getStorageTypeName() const override { return "SystemNumbers"; }

    UInt64 evaluateArgument(const Context & context, ASTPtr & argument) const;
};


}
