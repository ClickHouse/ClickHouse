#pragma once

#include "config.h"

#if USE_AVRO

#include <Storages/ObjectStorage/DataLakes/DataLakeConfigurationTemplate.h>
#include <Storages/ObjectStorage/DataLakes/Paimon/PaimonMetadata.h>

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
using StorageS3PaimonConfiguration = DataLakeConfiguration<StorageS3Configuration, PaimonMetadata>;
#endif

#if USE_AZURE_BLOB_STORAGE
using StorageAzurePaimonConfiguration = DataLakeConfiguration<StorageAzureConfiguration, PaimonMetadata>;
#endif

#if USE_HDFS
using StorageHDFSPaimonConfiguration = DataLakeConfiguration<StorageHDFSConfiguration, PaimonMetadata>;
#endif

using StorageLocalPaimonConfiguration = DataLakeConfiguration<StorageLocalConfiguration, PaimonMetadata>;

}

#endif
