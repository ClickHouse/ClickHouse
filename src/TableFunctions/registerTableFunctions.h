#pragma once

#include "config.h"

namespace DB
{
class TableFunctionFactory;
void registerTableFunctionMerge(TableFunctionFactory & factory);
void registerTableFunctionRemote(TableFunctionFactory & factory);
void registerTableFunctionNumbers(TableFunctionFactory & factory);
void registerTableFunctionPrimes(TableFunctionFactory & factory);
void registerTableFunctionLoop(TableFunctionFactory & factory);
void registerTableFunctionGenerateSeries(TableFunctionFactory & factory);
void registerTableFunctionNull(TableFunctionFactory & factory);
void registerTableFunctionZeros(TableFunctionFactory & factory);
void registerTableFunctionExecutable(TableFunctionFactory & factory);
void registerTableFunctionFile(TableFunctionFactory & factory);
void registerTableFunctionFileCluster(TableFunctionFactory & factory);
void registerTableFunctionURL(TableFunctionFactory & factory);
void registerTableFunctionURLCluster(TableFunctionFactory & factory);
void registerTableFunctionValues(TableFunctionFactory & factory);
void registerTableFunctionInput(TableFunctionFactory & factory);
void registerTableFunctionGenerate(TableFunctionFactory & factory);
#if USE_MONGODB
void registerTableFunctionMongoDB(TableFunctionFactory & factory);
#endif
#if USE_YTSAURUS
void registerTableFunctionYTsaurus(TableFunctionFactory & factory);
#endif
void registerTableFunctionRedis(TableFunctionFactory & factory);

#if USE_ARROWFLIGHT
void registerTableFunctionArrowFlight(TableFunctionFactory & factory);
#endif

void registerTableFunctionMergeTreeIndex(TableFunctionFactory & factory);
void registerTableFunctionMergeTreeAnalyzeIndexes(TableFunctionFactory & factory);
void registerTableFunctionMergeTreeProjection(TableFunctionFactory & factory);
void registerTableFunctionMergeTreeTextIndex(TableFunctionFactory & factory);
void registerTableFunctionFuzzQuery(TableFunctionFactory & factory);
#if USE_RAPIDJSON || USE_SIMDJSON
void registerTableFunctionFuzzJSON(TableFunctionFactory & factory);
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

void registerTableFunctionExplain(TableFunctionFactory & factory);

void registerTableFunctionObjectStorage(TableFunctionFactory & factory);
void registerTableFunctionObjectStorageCluster(TableFunctionFactory & factory);
void registerDataLakeTableFunctions(TableFunctionFactory & factory);
void registerDataLakeClusterTableFunctions(TableFunctionFactory & factory);

void registerTableFunctionTimeSeries(TableFunctionFactory & factory);

void registerTableFunctions();

}
