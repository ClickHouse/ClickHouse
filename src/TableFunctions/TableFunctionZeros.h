#pragma once

#include <TableFunctions/ITableFunction.h>
#include <Core/Types.h>


namespace DB
{

/* zeros(limit), zeros_mt(limit)
 * - the same as SELECT zero FROM system.zeros LIMIT limit.
 * Used for testing purposes, as a simple example of table function.
 */
template <bool multithreaded>
class TableFunctionZeros : public ITableFunction
{
public:
    static constexpr auto name = multithreaded ? "zeros_mt" : "zeros";
    std::string getName() const override { return name; }
private:
    StoragePtr executeImpl(const ASTPtr & ast_function, const Context & context, const std::string & table_name) const override;
    const char * getStorageTypeName() const override { return "SystemZeros"; }

    UInt64 evaluateArgument(const Context & context, ASTPtr & argument) const;
};


}
