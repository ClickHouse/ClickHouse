#include <TableFunctions/TableFunctionDirectory.h>
#include <TableFunctions/TableFunctionFactory.h>

namespace DB
{

void registerTableFunctionDirectory(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionDirectory>();
}
}
