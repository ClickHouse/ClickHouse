#pragma once
#include <Common/config.h>
#include "config_core.h"

namespace DB
{
void registerTableFunctions();

class TableFunctionFactory;

void registerTableFunctionMerge(TableFunctionFactory & factory);
void registerTableFunctionRemote(TableFunctionFactory & factory);
void registerTableFunctionNumbers(TableFunctionFactory & factory);
void registerTableFunctionFile(TableFunctionFactory & factory);
void registerTableFunctionS3(TableFunctionFactory & factory);
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


}
