#include <Storages/StorageFactory.h>
#include <Storages/registerStorages.h>

#include "config.h"

namespace DB
{

void registerStorageLog(StorageFactory & factory);
void registerStorageStripeLog(StorageFactory & factory);
void registerStorageMergeTree(StorageFactory & factory);
void registerStorageNull(StorageFactory & factory);
void registerStorageMerge(StorageFactory & factory);
void registerStorageBuffer(StorageFactory & factory);
void registerStorageDistributed(StorageFactory & factory);
void registerStorageMemory(StorageFactory & factory);
void registerStorageFile(StorageFactory & factory);
void registerStorageURL(StorageFactory & factory);
void registerStorageDictionary(StorageFactory & factory);
void registerStorageSet(StorageFactory & factory);
void registerStorageJoin(StorageFactory & factory);
void registerStorageView(StorageFactory & factory);
void registerStorageMaterializedView(StorageFactory & factory);
void registerStorageLiveView(StorageFactory & factory);
void registerStorageGenerateRandom(StorageFactory & factory);
void registerStorageExecutable(StorageFactory & factory);
void registerStorageWindowView(StorageFactory & factory);
void registerStorageLoop(StorageFactory & factory);
void registerStorageFuzzQuery(StorageFactory & factory);
void registerStorageTimeSeries(StorageFactory & factory);

#if USE_RAPIDJSON || USE_SIMDJSON
void registerStorageFuzzJSON(StorageFactory & factory);
#endif

#if USE_AWS_S3
void registerStorageS3(StorageFactory & factory);
void registerStorageHudi(StorageFactory & factory);
void registerStorageS3Queue(StorageFactory & factory);

#if USE_PARQUET
void registerStorageDeltaLake(StorageFactory & factory);
#endif
#if USE_AVRO
void registerStorageIceberg(StorageFactory & factory);
#endif
#endif

#if USE_AZURE_BLOB_STORAGE
void registerStorageAzureQueue(StorageFactory & factory);
#endif

#if USE_HDFS
#if USE_HIVE
void registerStorageHive(StorageFactory & factory);
#endif

#endif

void registerStorageODBC(StorageFactory & factory);
void registerStorageJDBC(StorageFactory & factory);

#if USE_MYSQL
void registerStorageMySQL(StorageFactory & factory);
#endif

#if USE_MONGODB
void registerStorageMongoDB(StorageFactory & factory);
void registerStorageMongoDBPocoLegacy(StorageFactory & factory);
#endif

void registerStorageRedis(StorageFactory & factory);


#if USE_RDKAFKA
void registerStorageKafka(StorageFactory & factory);
#endif

#if USE_AMQPCPP
void registerStorageRabbitMQ(StorageFactory & factory);
#endif

#if USE_NATSIO
void registerStorageNATS(StorageFactory & factory);
#endif

#if USE_ROCKSDB
void registerStorageEmbeddedRocksDB(StorageFactory & factory);
#endif

#if USE_LIBPQXX
void registerStoragePostgreSQL(StorageFactory & factory);
void registerStorageMaterializedPostgreSQL(StorageFactory & factory);
#endif

#if USE_MYSQL || USE_LIBPQXX
void registerStorageExternalDistributed(StorageFactory & factory);
#endif

#if USE_FILELOG
void registerStorageFileLog(StorageFactory & factory);
#endif

#if USE_SQLITE
void registerStorageSQLite(StorageFactory & factory);
#endif

void registerStorageKeeperMap(StorageFactory & factory);

void registerStorageObjectStorage(StorageFactory & factory);

void registerStorages(bool use_legacy_mongodb_integration [[maybe_unused]])
{
    auto & factory = StorageFactory::instance();

    registerStorageLog(factory);
    registerStorageStripeLog(factory);
    registerStorageMergeTree(factory);
    registerStorageNull(factory);
    registerStorageMerge(factory);
    registerStorageBuffer(factory);
    registerStorageDistributed(factory);
    registerStorageMemory(factory);
    registerStorageFile(factory);
    registerStorageURL(factory);
    registerStorageDictionary(factory);
    registerStorageSet(factory);
    registerStorageJoin(factory);
    registerStorageView(factory);
    registerStorageMaterializedView(factory);
    registerStorageLiveView(factory);
    registerStorageGenerateRandom(factory);
    registerStorageExecutable(factory);
    registerStorageWindowView(factory);
    registerStorageLoop(factory);
    registerStorageFuzzQuery(factory);
    registerStorageTimeSeries(factory);

#if USE_RAPIDJSON || USE_SIMDJSON
    registerStorageFuzzJSON(factory);
#endif

#if USE_AZURE_BLOB_STORAGE
    registerStorageAzureQueue(factory);
#endif

#if USE_AWS_S3
    registerStorageHudi(factory);
    registerStorageS3Queue(factory);

    #if USE_PARQUET
    registerStorageDeltaLake(factory);
    #endif

    #if USE_AVRO
    registerStorageIceberg(factory);
    #endif

    #endif

    #if USE_HDFS
    #if USE_HIVE
    registerStorageHive(factory);
    #endif
    #endif

    registerStorageODBC(factory);
    registerStorageJDBC(factory);

    #if USE_MYSQL
    registerStorageMySQL(factory);
    #endif

    #if USE_MONGODB
    if (use_legacy_mongodb_integration)
        registerStorageMongoDBPocoLegacy(factory);
    else
        registerStorageMongoDB(factory);
    #endif

    registerStorageRedis(factory);

    #if USE_RDKAFKA
    registerStorageKafka(factory);
    #endif

    #if USE_FILELOG
    registerStorageFileLog(factory);
    #endif

    #if USE_AMQPCPP
    registerStorageRabbitMQ(factory);
    #endif

    #if USE_NATSIO
    registerStorageNATS(factory);
    #endif

    #if USE_ROCKSDB
    registerStorageEmbeddedRocksDB(factory);
    #endif

    #if USE_LIBPQXX
    registerStoragePostgreSQL(factory);
    registerStorageMaterializedPostgreSQL(factory);
    #endif

    #if USE_MYSQL || USE_LIBPQXX
    registerStorageExternalDistributed(factory);
    #endif

    #if USE_SQLITE
    registerStorageSQLite(factory);
    #endif

    registerStorageKeeperMap(factory);

    registerStorageObjectStorage(factory);
}

}
