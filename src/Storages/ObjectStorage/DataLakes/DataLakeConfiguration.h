#pragma once

#include "config.h"

#include <Storages/ObjectStorage/DataLakes/DataLakeStorageSettings.h>
#include <Storages/ObjectStorage/DataLakes/IDataLakeMetadata.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadata.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLakeMetadata.h>
#include <Storages/ObjectStorage/DataLakes/Paimon/PaimonMetadata.h>
#include <Storages/ObjectStorage/DataLakes/HudiMetadata.h>
#include <Storages/ObjectStorage/Azure/Configuration.h>
#include <Storages/ObjectStorage/HDFS/Configuration.h>
#include <Storages/ObjectStorage/Local/Configuration.h>
#include <Storages/ObjectStorage/S3/Configuration.h>
#include <Storages/ObjectStorage/StorageObjectStorageConfiguration.h>

#include <concepts>
#include <string>


namespace DB
{

template <typename T>
concept StorageConfiguration = std::derived_from<T, StorageObjectStorageConfiguration>;

/// DataLakeConfiguration is a thin wrapper around a base storage configuration
/// (S3, Azure, etc.) that marks it as a data lake configuration and adjusts the
/// engine name and path. All metadata management is handled externally by
/// StorageDataLake / StorageDataLakeCluster.
///
/// Settings are injected via setDataLakeSettings by the storage classes and
/// accessed by metadata implementations via the configuration weak pointer.
template <StorageConfiguration BaseStorageConfiguration, typename DataLakeMetadata>
class DataLakeConfiguration : public BaseStorageConfiguration
{
public:
    using MetadataType = DataLakeMetadata;

    explicit DataLakeConfiguration(DataLakeStorageSettingsPtr settings_ = nullptr)
        : settings(std::move(settings_))
    {
    }

    const DataLakeStorageSettings & getDataLakeSettings() const override
    {
        chassert(settings);
        return *settings;
    }

    void setDataLakeSettings(DataLakeStorageSettingsPtr settings_) override { settings = std::move(settings_); }

    std::string getEngineName() const override { return DataLakeMetadata::name + BaseStorageConfiguration::getEngineName(); }

    StorageObjectStorageConfiguration::Path getRawPath() const override
    {
        auto result = BaseStorageConfiguration::getRawPath().path;
        return StorageObjectStorageConfiguration::Path(result.ends_with('/') ? result : result + "/");
    }

private:
    DataLakeStorageSettingsPtr settings;
};


#if USE_AVRO
#    if USE_AWS_S3
using StorageS3IcebergConfiguration = DataLakeConfiguration<StorageS3Configuration, IcebergMetadata>;
using StorageS3PaimonConfiguration = DataLakeConfiguration<StorageS3Configuration, PaimonMetadata>;
#endif

#if USE_AZURE_BLOB_STORAGE
using StorageAzureIcebergConfiguration = DataLakeConfiguration<StorageAzureConfiguration, IcebergMetadata>;
using StorageAzurePaimonConfiguration = DataLakeConfiguration<StorageAzureConfiguration, PaimonMetadata>;
#endif

#if USE_HDFS
using StorageHDFSIcebergConfiguration = DataLakeConfiguration<StorageHDFSConfiguration, IcebergMetadata>;
using StorageHDFSPaimonConfiguration = DataLakeConfiguration<StorageHDFSConfiguration, PaimonMetadata>;
#endif

using StorageLocalIcebergConfiguration = DataLakeConfiguration<StorageLocalConfiguration, IcebergMetadata>;
using StorageLocalPaimonConfiguration = DataLakeConfiguration<StorageLocalConfiguration, PaimonMetadata>;
#endif

#if USE_PARQUET
#if USE_AWS_S3
using StorageS3DeltaLakeConfiguration = DataLakeConfiguration<StorageS3Configuration, DeltaLakeMetadata>;
#endif

#if USE_AZURE_BLOB_STORAGE
using StorageAzureDeltaLakeConfiguration = DataLakeConfiguration<StorageAzureConfiguration, DeltaLakeMetadata>;
#endif

using StorageLocalDeltaLakeConfiguration = DataLakeConfiguration<StorageLocalConfiguration, DeltaLakeMetadata>;

#endif

#if USE_AWS_S3
using StorageS3HudiConfiguration = DataLakeConfiguration<StorageS3Configuration, HudiMetadata>;
#endif
}
