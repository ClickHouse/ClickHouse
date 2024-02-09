#pragma once

#include <Storages/IStorage.h>
#include <Storages/DataLakes/IStorageDataLake.h>
#include <Storages/DataLakes/DeltaLakeMetadataParser.h>
#include "config.h"

namespace DB
{

struct StorageDeltaLakeName
{
    static constexpr auto name = "DeltaLake";
};

#if USE_AWS_S3 && USE_PARQUET
using StorageDeltaLakeS3 = IStorageDataLake<S3StorageSettings, StorageDeltaLakeName, DeltaLakeMetadataParser>;
#endif

}
