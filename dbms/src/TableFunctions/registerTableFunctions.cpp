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
#if Poco_DataODBC_FOUND
void registerTableFunctionODBC(TableFunctionFactory & factory);
#endif

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

#if Poco_DataODBC_FOUND
    registerTableFunctionODBC(factory);
#endif

#if USE_MYSQL
    registerTableFunctionMySQL(factory);
#endif
}

}
