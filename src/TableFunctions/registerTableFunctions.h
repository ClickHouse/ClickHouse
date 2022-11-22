#pragma once

#include <Common/config.h>
#include "config_core.h"

namespace DB
{
class TableFunctionFactory;
void registerTableFunctionMerge(TableFunctionFactory & factory);
void registerTableFunctionRemote(TableFunctionFactory & factory);
void registerTableFunctionNumbers(TableFunctionFactory & factory);
void registerTableFunctionNull(TableFunctionFactory & factory);
void registerTableFunctionZeros(TableFunctionFactory & factory);
void registerTableFunctionExecutable(TableFunctionFactory & factory);
void registerTableFunctionFile(TableFunctionFactory & factory);
void registerTableFunctionURL(TableFunctionFactory & factory);
void registerTableFunctionValues(TableFunctionFactory & factory);
void registerTableFunctionInput(TableFunctionFactory & factory);
void registerTableFunctionGenerate(TableFunctionFactory & factory);
void registerTableFunctionMongoDB(TableFunctionFactory & factory);

void registerTableFunctionMeiliSearch(TableFunctionFactory & factory);

#if USE_AWS_S3
void registerTableFunctionS3(TableFunctionFactory & factory);
void registerTableFunctionS3Cluster(TableFunctionFactory & factory);
void registerTableFunctionCOS(TableFunctionFactory & factory);
#endif

#if USE_HDFS
void registerTableFunctionHDFS(TableFunctionFactory & factory);
void registerTableFunctionHDFSCluster(TableFunctionFactory & factory);
#endif

#if USE_HIVE
void registerTableFunctionHive(TableFunctionFactory & factory);
#endif

void registerTableFunctionODBC(TableFunctionFactory & factory);
void registerTableFunctionJDBC(TableFunctionFactory & factory);

void registerTableFunctionView(TableFunctionFactory & factory);
void registerTableFunctionViewIfPermitted(TableFunctionFactory & factory);

#if USE_MYSQL
void registerTableFunctionMySQL(TableFunctionFactory & factory);
#endif

#if USE_LIBPQXX
void registerTableFunctionPostgreSQL(TableFunctionFactory & factory);
#endif

#if USE_SQLITE
void registerTableFunctionSQLite(TableFunctionFactory & factory);
#endif

void registerTableFunctionDictionary(TableFunctionFactory & factory);

void registerTableFunctionFormat(TableFunctionFactory & factory);

void registerTableFunctions();

}
