#pragma once

#include "config.h"

#if USE_AWS_S3 && USE_GOOGLE_CLOUD

#include <Storages/ObjectStorage/S3/Configuration.h>

namespace DB
{

/// Native Google Cloud Storage configuration for the `gcs` table function and the `GCS` table engine,
/// selected when the `use_native_gcs` setting is enabled.
///
/// It reuses all of StorageS3Configuration's argument parsing (the argument grammar is identical to
/// `s3()`), but reports the GCS object-storage type and builds a native GCSObjectStorage over
/// google-cloud-cpp instead of an S3-compatible S3ObjectStorage.
class StorageGCSConfiguration : public StorageS3Configuration
{
public:
    static constexpr auto type = ObjectStorageType::GCS;
    static constexpr auto type_name = "gcs";

    StorageGCSConfiguration() = default;

    ObjectStorageType getType() const override { return type; }
    std::string getTypeName() const override { return type_name; }

    ObjectStoragePtr createObjectStorage(
        ContextPtr context, bool is_readonly, CredentialsConfigurationCallback refresh_credentials_callback) override;
};

}

#endif
