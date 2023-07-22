#pragma once

#include <Storages/IStorage.h>
#include <Storages/DataLakes/IStorageDataLake.h>
#include <Storages/DataLakes/HudiMetadataParser.h>
#include "config.h"

#if USE_AWS_S3
#include <Storages/DataLakes/S3MetadataReader.h>
#include <Storages/StorageS3.h>
#endif

namespace DB
{

struct StorageHudiName
{
    static constexpr auto name = "Hudi";
};

#if USE_AWS_S3
using StorageHudiS3 = IStorageDataLake<StorageS3, StorageHudiName, HudiMetadataParser<StorageS3::Configuration, S3DataLakeMetadataReadHelper>>;
#endif

}
