#include <Storages/registerStorages.h>
#include <Storages/StorageFactory.h>

#if !defined(ARCADIA_BUILD)
#    include <Common/config.h>
#    include "config_core.h"
#endif

namespace DB
{

void registerStorageLog(StorageFactory & factory);
void registerStorageTinyLog(StorageFactory & factory);
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

#if USE_AWS_S3
void registerStorageS3(StorageFactory & factory);
void registerStorageCOS(StorageFactory & factory);
#endif

#if USE_HDFS
void registerStorageHDFS(StorageFactory & factory);
#endif

void registerStorageODBC(StorageFactory & factory);
void registerStorageJDBC(StorageFactory & factory);

#if USE_MYSQL
void registerStorageMySQL(StorageFactory & factory);
#endif

void registerStorageMongoDB(StorageFactory & factory);

#if USE_RDKAFKA
void registerStorageKafka(StorageFactory & factory);
#endif

#if USE_AMQPCPP
void registerStorageRabbitMQ(StorageFactory & factory);
#endif

#if USE_ROCKSDB
void registerStorageEmbeddedRocksDB(StorageFactory & factory);
#endif


void registerStorages()
{
    auto & factory = StorageFactory::instance();

    registerStorageLog(factory);
    registerStorageTinyLog(factory);
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

    #if USE_AWS_S3
    registerStorageS3(factory);
    registerStorageCOS(factory);
    #endif

    #if USE_HDFS
    registerStorageHDFS(factory);
    #endif

    registerStorageODBC(factory);
    registerStorageJDBC(factory);

    #if USE_MYSQL
    registerStorageMySQL(factory);
    #endif

    registerStorageMongoDB(factory);

    #if USE_RDKAFKA
    registerStorageKafka(factory);
    #endif

    #if USE_AMQPCPP
    registerStorageRabbitMQ(factory);
    #endif

    #if USE_ROCKSDB
    registerStorageEmbeddedRocksDB(factory);
    #endif
}

}
