#include "registerTableFunctions.h"
#include <TableFunctions/TableFunctionFactory.h>

namespace DB
{
void registerTableFunctions(bool use_legacy_mongodb_integration [[maybe_unused]])
{
    auto & factory = TableFunctionFactory::instance();

    registerTableFunctionMerge(factory);
    registerTableFunctionRemote(factory);
    registerTableFunctionNumbers(factory);
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
    if (use_legacy_mongodb_integration)
        registerTableFunctionMongoDBPocoLegacy(factory);
    else
        registerTableFunctionMongoDB(factory);
#endif
    registerTableFunctionRedis(factory);
    registerTableFunctionMergeTreeIndex(factory);
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
}

}
