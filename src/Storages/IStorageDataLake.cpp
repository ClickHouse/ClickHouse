#include <Storages/IStorageDataLake.h>
#include "config.h"

#if USE_AWS_S3

#include <Storages/StorageHudi.h>
#include <Storages/StorageDeltaLake.h>
#include <Storages/StorageIceberg.h>


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
    REGISTER_DATA_LAKE_STORAGE(StorageDeltaLake, StorageDeltaLakeName::name)
}

void registerStorageIceberg(StorageFactory & factory)
{
    REGISTER_DATA_LAKE_STORAGE(StorageIceberg, StorageIcebergName::name)
}

void registerStorageHudi(StorageFactory & factory)
{
    REGISTER_DATA_LAKE_STORAGE(StorageHudi, StorageHudiName::name)
}

}

#endif
