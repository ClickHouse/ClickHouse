#pragma once

#include "config.h"

#if USE_PARQUET

#include <Storages/ObjectStorage/DataLakes/DataLakeConfigurationTemplate.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLakeMetadata.h>

#if USE_AWS_S3
#include <Storages/ObjectStorage/S3/Configuration.h>
#endif

#if USE_AZURE_BLOB_STORAGE
#include <Storages/ObjectStorage/Azure/Configuration.h>
#endif

#include <Storages/ObjectStorage/Local/Configuration.h>

namespace DB
{

#if USE_AWS_S3
using StorageS3DeltaLakeConfiguration = DataLakeConfiguration<StorageS3Configuration, DeltaLakeMetadata>;
#endif

#if USE_AZURE_BLOB_STORAGE
using StorageAzureDeltaLakeConfiguration = DataLakeConfiguration<StorageAzureConfiguration, DeltaLakeMetadata>;
#endif

using StorageLocalDeltaLakeConfiguration = DataLakeConfiguration<StorageLocalConfiguration, DeltaLakeMetadata>;

}

#endif
