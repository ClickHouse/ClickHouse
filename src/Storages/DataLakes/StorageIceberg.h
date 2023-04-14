#pragma once

#include <Storages/IStorage.h>
#include <Storages/DataLakes/IStorageDataLake.h>
#include <Storages/DataLakes/IcebergMetadataParser.h>
#include "config.h"

#if USE_AWS_S3
#include <Storages/DataLakes/S3MetadataReader.h>
#include <Storages/StorageS3.h>
#endif

namespace DB
{

struct StorageIcebergName
{
    static constexpr auto name = "Iceberg";
};

#if USE_AWS_S3
using StorageIcebergS3 = IStorageDataLake<StorageS3, StorageIcebergName, IcebergMetadataParser<StorageS3::Configuration, S3DataLakeMetadataReadHelper>>;
#endif

}
