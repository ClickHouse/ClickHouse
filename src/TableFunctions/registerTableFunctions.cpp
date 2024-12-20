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
    registerTableFunctionGenerateSeries(factory);
    registerTableFunctionNull(factory);
    registerTableFunctionZeros(factory);
#if REGISTER_EXECUTABLE_FUNCTION
    registerTableFunctionExecutable(factory);
#endif
#if REGISTER_FILE_FUNCTION
    registerTableFunctionFile(factory);
    registerTableFunctionFileCluster(factory);
#endif
#if REGISTER_URL_FUNCTION
    registerTableFunctionURL(factory);
#if REGISTER_URL_CLUSTER_FUNCTION
    registerTableFunctionURLCluster(factory);
#endif
#endif
    registerTableFunctionValues(factory);
    registerTableFunctionInput(factory);
    registerTableFunctionGenerate(factory);
#if REGISTER_MONGODB_FUNCTION
    registerTableFunctionMongoDB(factory);
#endif
#if REGISTER_REDIS_FUNCTION
    registerTableFunctionRedis(factory);
#endif
    registerTableFunctionMergeTreeIndex(factory);
#if USE_RAPIDJSON || USE_SIMDJSON
    registerTableFunctionFuzzJSON(factory);
#endif

#if USE_AWS_S3 && REGISTER_S3_FUNCTION
    registerTableFunctionS3(factory);
    registerTableFunctionS3Cluster(factory);
    registerTableFunctionCOS(factory);
    registerTableFunctionOSS(factory);
    registerTableFunctionGCS(factory);
    registerTableFunctionHudi(factory);
#if USE_PARQUET
    registerTableFunctionDeltaLake(factory);
#endif
#if USE_AVRO
    registerTableFunctionIceberg(factory);
#endif

#endif

#if USE_HDFS && REGISTER_HDFS_FUNCTION
    registerTableFunctionHDFS(factory);
    registerTableFunctionHDFSCluster(factory);
#endif

#if USE_HIVE && REGISTER_HIVE_FUNCTION
    registerTableFunctionHive(factory);
#endif

#if REGISTER_ODBC_FUNCTION
    registerTableFunctionODBC(factory);
    registerTableFunctionJDBC(factory);
#endif

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

#if USE_AZURE_BLOB_STORAGE && REGISTER_AZURE_BLOB_FUNCTION
    registerTableFunctionAzureBlobStorage(factory);
    registerTableFunctionAzureBlobStorageCluster(factory);
#endif


}

}
