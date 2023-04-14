#include <Storages/DataLakes/IStorageDataLake.h>
#include "config.h"

#if USE_AWS_S3

#include <Storages/DataLakes/StorageDeltaLake.h>
#include <Storages/DataLakes/StorageIceberg.h>
#include <Storages/DataLakes/StorageHudi.h>


namespace DB
{

#define REGISTER_DATA_LAKE_STORAGE(STORAGE, NAME)       \
    factory.registerStorage(                            \
        NAME,                                           \
        [](const StorageFactory::Arguments & args)      \
        {                                               \
            return createDataLakeStorage<STORAGE>(args);\
        },                                              \
        {                                               \
            .supports_settings = false,                 \
            .supports_schema_inference = true,          \
            .source_access_type = AccessType::S3,       \
        });


void registerStorageDeltaLake(StorageFactory & factory)
{
    REGISTER_DATA_LAKE_STORAGE(StorageDeltaLakeS3, StorageDeltaLakeName::name)
}

void registerStorageIceberg(StorageFactory & factory)
{
    REGISTER_DATA_LAKE_STORAGE(StorageIcebergS3, StorageIcebergName::name)
}

void registerStorageHudi(StorageFactory & factory)
{
    REGISTER_DATA_LAKE_STORAGE(StorageHudiS3, StorageHudiName::name)
}

}

#endif
