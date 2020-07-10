#include <Common/config.h>

#if USE_AWS_S3

#include <IO/S3Common.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/TableFunctionCOS.h>
#include "registerTableFunctions.h"

namespace DB
{

void registerTableFunctionCOS(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionCOS>();
}

}

#endif
