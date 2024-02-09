#pragma once

#include <Storages/IStorage.h>
#include <Storages/DataLakes/IStorageDataLake.h>
#include <Storages/DataLakes/HudiMetadataParser.h>
#include "config.h"

namespace DB
{

struct StorageHudiName
{
    static constexpr auto name = "Hudi";
};

#if USE_AWS_S3
using StorageHudiS3 = IStorageDataLake<S3StorageSettings, StorageHudiName, HudiMetadataParser>;
#endif

}
