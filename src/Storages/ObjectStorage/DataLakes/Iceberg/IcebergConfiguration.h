#pragma once

#include "config.h"

#if USE_AVRO

#include <Storages/ObjectStorage/DataLakes/DataLakeConfigurationTemplate.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadata.h>

#if USE_AWS_S3
#include <Storages/ObjectStorage/S3/Configuration.h>
#endif

#if USE_AZURE_BLOB_STORAGE
#include <Storages/ObjectStorage/Azure/Configuration.h>
#endif

#if USE_HDFS
#include <Storages/ObjectStorage/HDFS/Configuration.h>
#endif

#include <Storages/ObjectStorage/Local/Configuration.h>

namespace DB
{

#if USE_AWS_S3
using StorageS3IcebergConfiguration = DataLakeConfiguration<StorageS3Configuration, IcebergMetadata>;
#endif

#if USE_AZURE_BLOB_STORAGE
using StorageAzureIcebergConfiguration = DataLakeConfiguration<StorageAzureConfiguration, IcebergMetadata>;
#endif

#if USE_HDFS
using StorageHDFSIcebergConfiguration = DataLakeConfiguration<StorageHDFSConfiguration, IcebergMetadata>;
#endif

using StorageLocalIcebergConfiguration = DataLakeConfiguration<StorageLocalConfiguration, IcebergMetadata>;

}

#endif
