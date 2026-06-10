#pragma once

#include "config.h"

#if USE_AWS_S3

#include <Storages/ObjectStorage/DataLakes/DataLakeConfigurationTemplate.h>
#include <Storages/ObjectStorage/DataLakes/HudiMetadata.h>
#include <Storages/ObjectStorage/S3/Configuration.h>

namespace DB
{

using StorageS3HudiConfiguration = DataLakeConfiguration<StorageS3Configuration, HudiMetadata>;

}

#endif
