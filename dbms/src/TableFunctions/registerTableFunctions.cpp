#include <Common/config.h>
#include "config_core.h"
#include <TableFunctions/registerTableFunctions.h>
#include <TableFunctions/TableFunctionFactory.h>


namespace DB
{

void registerTableFunctionMerge(TableFunctionFactory & factory);
void registerTableFunctionRemote(TableFunctionFactory & factory);
void registerTableFunctionNumbers(TableFunctionFactory & factory);
void registerTableFunctionFile(TableFunctionFactory & factory);
void registerTableFunctionURL(TableFunctionFactory & factory);
void registerTableFunctionValues(TableFunctionFactory & factory);
void registerTableFunctionInput(TableFunctionFactory & factory);

#if USE_HDFS
void registerTableFunctionHDFS(TableFunctionFactory & factory);
#endif

#if USE_POCO_SQLODBC || USE_POCO_DATAODBC
void registerTableFunctionODBC(TableFunctionFactory & factory);
#endif

void registerTableFunctionJDBC(TableFunctionFactory & factory);

#if USE_MYSQL
void registerTableFunctionMySQL(TableFunctionFactory & factory);
#endif


void registerTableFunctions()
{
    auto & factory = TableFunctionFactory::instance();

    registerTableFunctionMerge(factory);
    registerTableFunctionRemote(factory);
    registerTableFunctionNumbers(factory);
    registerTableFunctionFile(factory);
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
