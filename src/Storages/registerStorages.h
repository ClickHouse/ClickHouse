#pragma once

#if !defined(ARCADIA_BUILD)
#    include <Common/config.h>
#    include "config_core.h"
#endif

namespace DB
{
class StorageFactory;

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

void registerStorages();

}
