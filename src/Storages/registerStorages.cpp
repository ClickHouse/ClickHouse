#include <Storages/registerStorages.h>
#include <Storages/StorageFactory.h>


namespace DB
{

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
}

}
