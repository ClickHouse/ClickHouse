#include <Common/config.h>
#include <TableFunctions/registerTableFunctions.h>
#include <TableFunctions/TableFunctionFactory.h>


namespace DB
{

void registerTableFunctionMerge(TableFunctionFactory & factory);
void registerTableFunctionRemote(TableFunctionFactory & factory);
void registerTableFunctionShardByHash(TableFunctionFactory & factory);
void registerTableFunctionNumbers(TableFunctionFactory & factory);
void registerTableFunctionCatBoostPool(TableFunctionFactory & factory);
void registerTableFunctionFile(TableFunctionFactory & factory);
void registerTableFunctionURL(TableFunctionFactory & factory);
void registerTableFunctionHDFS(TableFunctionFactory & factory);

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
    registerTableFunctionShardByHash(factory);
    registerTableFunctionNumbers(factory);
    registerTableFunctionCatBoostPool(factory);
    registerTableFunctionFile(factory);
    registerTableFunctionURL(factory);
    registerTableFunctionHDFS(factory);

#if USE_POCO_SQLODBC || USE_POCO_DATAODBC
    registerTableFunctionODBC(factory);
#endif
    registerTableFunctionJDBC(factory);

#if USE_MYSQL
    registerTableFunctionMySQL(factory);
#endif
}

}
