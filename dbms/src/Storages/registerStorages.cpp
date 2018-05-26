#include <Common/config.h>

#include <Storages/registerStorages.h>
#include <Storages/StorageFactory.h>


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
void registerStorageDictionary(StorageFactory & factory);
void registerStorageSet(StorageFactory & factory);
void registerStorageJoin(StorageFactory & factory);
void registerStorageView(StorageFactory & factory);
void registerStorageMaterializedView(StorageFactory & factory);

#if USE_POCO_SQLODBC || USE_POCO_DATAODBC
void registerStorageODBC(StorageFactory & factory);
#endif

#if USE_MYSQL
void registerStorageMySQL(StorageFactory & factory);
#endif

#if USE_RDKAFKA
void registerStorageKafka(StorageFactory & factory);
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
    registerStorageDictionary(factory);
    registerStorageSet(factory);
    registerStorageJoin(factory);
    registerStorageView(factory);
    registerStorageMaterializedView(factory);

    #if USE_POCO_SQLODBC || USE_POCO_DATAODBC
    registerStorageODBC(factory);
    #endif

    #if USE_MYSQL
    registerStorageMySQL(factory);
    #endif

    #if USE_RDKAFKA
    registerStorageKafka(factory);
    #endif
}

}
