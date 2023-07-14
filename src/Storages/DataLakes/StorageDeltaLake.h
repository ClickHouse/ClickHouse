#pragma once

#include <Storages/IStorage.h>
#include <Storages/DataLakes/IStorageDataLake.h>
#include <Storages/DataLakes/DeltaLakeMetadataParser.h>
#include "config.h"

#if USE_AWS_S3
#include <Storages/DataLakes/S3MetadataReader.h>
#include <Storages/StorageS3.h>
#endif

namespace DB
{

struct StorageDeltaLakeName
{
    static constexpr auto name = "DeltaLake";
};

#if USE_AWS_S3 && USE_PARQUET
using StorageDeltaLakeS3 = IStorageDataLake<StorageS3, StorageDeltaLakeName, DeltaLakeMetadataParser<StorageS3::Configuration, S3DataLakeMetadataReadHelper>>;
#endif

}
