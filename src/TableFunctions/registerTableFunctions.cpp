#include <TableFunctions/registerTableFunctions.h>
#include <TableFunctions/TableFunctionFactory.h>

namespace DB
{
void registerTableFunctions()
{
    auto & factory = TableFunctionFactory::instance();

    registerTableFunctionMerge(factory);
    registerTableFunctionRemote(factory);
    registerTableFunctionNumbers(factory);
    registerTableFunctionPrimes(factory);
    registerTableFunctionLoop(factory);
    registerTableFunctionGenerateSeries(factory);
    registerTableFunctionNull(factory);
    registerTableFunctionZeros(factory);
    registerTableFunctionExecutable(factory);
    registerTableFunctionFile(factory);
    registerTableFunctionFileCluster(factory);
    registerTableFunctionURL(factory);
    registerTableFunctionURLCluster(factory);
    registerTableFunctionValues(factory);
    registerTableFunctionInput(factory);
    registerTableFunctionGenerate(factory);
#if USE_MONGODB
    registerTableFunctionMongoDB(factory);
#endif
    registerTableFunctionRedis(factory);

#if USE_ARROWFLIGHT
    registerTableFunctionArrowFlight(factory);
#endif

    registerTableFunctionMergeTreeIndex(factory);
    registerTableFunctionMergeTreeAnalyzeIndexes(factory);
    registerTableFunctionMergeTreeProjection(factory);
    registerTableFunctionMergeTreeTextIndex(factory);
    registerTableFunctionFuzzQuery(factory);
#if USE_RAPIDJSON || USE_SIMDJSON
    registerTableFunctionFuzzJSON(factory);
#endif

#if USE_HIVE
    registerTableFunctionHive(factory);
#endif

    registerTableFunctionODBC(factory);
    registerTableFunctionJDBC(factory);

    registerTableFunctionView(factory);
    registerTableFunctionViewIfPermitted(factory);

#if USE_MYSQL
    registerTableFunctionMySQL(factory);
#endif

#if USE_LIBPQXX
    registerTableFunctionPostgreSQL(factory);
#endif

#if USE_SQLITE
    registerTableFunctionSQLite(factory);
#endif

    registerTableFunctionDictionary(factory);

    registerTableFunctionFormat(factory);
    registerTableFunctionExplain(factory);
    registerTableFunctionTimeSeries(factory);

    registerTableFunctionObjectStorage(factory);
    registerTableFunctionObjectStorageCluster(factory);
    registerDataLakeTableFunctions(factory);
    registerDataLakeClusterTableFunctions(factory);

#if USE_YTSAURUS
    registerTableFunctionYTsaurus(factory);
#endif

}

}
