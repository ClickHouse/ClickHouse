#include "registerTableFunctions.h"
#include <TableFunctions/TableFunctionFactory.h>


namespace DB
{
void registerTableFunctions()
{
    auto & factory = TableFunctionFactory::instance();

    registerTableFunctionMerge(factory);
    registerTableFunctionRemote(factory);
    registerTableFunctionNumbers(factory);
    registerTableFunctionFile(factory);
    registerTableFunctionS3(factory);
    registerTableFunctionURL(factory);
    registerTableFunctionValues(factory);
    registerTableFunctionInput(factory);

#if USE_HDFS
    registerTableFunctionHDFS(factory);
#endif

#if USE_POCO_SQLODBC || USE_POCO_DATAODBC
    registerTableFunctionODBC(factory);
#endif
    registerTableFunctionJDBC(factory);

#if USE_MYSQL
    registerTableFunctionMySQL(factory);
#endif
}

}
