#pragma once

#include "config.h"

#if USE_AWS_S3 && USE_GOOGLE_CLOUD

#include <Storages/ObjectStorage/S3/Configuration.h>
#include <Disks/DiskObjectStorage/ObjectStorages/GCS/gcsSettings.h>

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

    /// A `disk = '...'` setting takes the whole backend from a native GCS disk. The inherited
    /// implementation reads the disk's `S3ObjectStorage`, which a GCS disk is not, so it is replaced
    /// rather than reused.
    void fromDisk(const String & disk_name, ASTs & args, ContextPtr context, bool with_structure) override;

private:
    /// Set only by `fromDisk`: the settings of the disk that backs this configuration. They already
    /// carry the endpoint and the credentials, so `createObjectStorage` uses them as they are instead
    /// of translating the `s3(...)` argument grammar.
    std::shared_ptr<const GCSObjectStorageSettings> disk_settings;
    String backing_disk_name;
};

}

#endif
