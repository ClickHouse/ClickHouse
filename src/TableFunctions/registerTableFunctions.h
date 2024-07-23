#pragma once

#include "config.h"

namespace DB
{
class TableFunctionFactory;
void registerTableFunctionMerge(TableFunctionFactory & factory);
void registerTableFunctionRemote(TableFunctionFactory & factory);
void registerTableFunctionNumbers(TableFunctionFactory & factory);
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
void registerTableFunctionMongoDB(TableFunctionFactory & factory);
void registerTableFunctionRedis(TableFunctionFactory & factory);
void registerTableFunctionMergeTreeIndex(TableFunctionFactory & factory);
#if USE_RAPIDJSON || USE_SIMDJSON
void registerTableFunctionFuzzJSON(TableFunctionFactory & factory);
#endif

#if USE_AWS_S3
void registerTableFunctionS3(TableFunctionFactory & factory);
void registerTableFunctionS3Cluster(TableFunctionFactory & factory);
void registerTableFunctionCOS(TableFunctionFactory & factory);
void registerTableFunctionOSS(TableFunctionFactory & factory);
void registerTableFunctionGCS(TableFunctionFactory & factory);
void registerTableFunctionHudi(TableFunctionFactory & factory);
#if USE_PARQUET
void registerTableFunctionDeltaLake(TableFunctionFactory & factory);
#endif
#if USE_AVRO
void registerTableFunctionIceberg(TableFunctionFactory & factory);
#endif
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

void registerTableFunctionExplain(TableFunctionFactory & factory);

#if USE_AZURE_BLOB_STORAGE
void registerTableFunctionAzureBlobStorage(TableFunctionFactory & factory);
void registerTableFunctionAzureBlobStorageCluster(TableFunctionFactory & factory);
#endif

void registerTableFunctions();

}
