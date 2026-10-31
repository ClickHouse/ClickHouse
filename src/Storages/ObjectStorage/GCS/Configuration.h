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

    /// The native object storage reads every key from the bucket it was created for, so a fully
    /// qualified path cannot name another bucket. Iceberg paths in another bucket are rejected by
    /// `IcebergPathResolver` instead.
    bool supportsFullyQualifiedPaths() const override { return false; }

    ObjectStoragePtr createObjectStorage(
        ContextPtr context, bool is_readonly, CredentialsConfigurationCallback refresh_credentials_callback) override;

    /// A `disk = '...'` setting takes the whole backend from a native GCS disk. The inherited
    /// implementation reads the disk's `S3ObjectStorage`, which a GCS disk is not, so it is replaced
    /// rather than reused.
    void fromDisk(const String & disk_name, ASTs & args, ContextPtr context, bool with_structure) override;

protected:
    /// The inherited parser silently ignores the key-value arguments that only matter to the native
    /// backend (`google_adc_*`, including `google_adc_token_uri`, and `use_environment_credentials`),
    /// most of which named collections do carry.
    /// Read them on top of it, so that the bare-URL form authenticates the same way.
    void fromAST(ASTs & args, ContextPtr context, bool with_structure) override;
    void fromNamedCollection(const NamedCollection & collection, ContextPtr context) override;

private:
    /// The inherited `S3::URI` parser does not know the virtual-hosted form of the GCS host: with the default
    /// `s3_uri_style = AUTO` it splits `https://bucket-a.storage.googleapis.com/bucket-b/file` as path-style,
    /// taking `bucket-b` as the bucket. The native client always talks to `storage.googleapis.com`, so take the
    /// bucket from the host and the key from the whole path, as `parseGCSEndpoint` does for a disk.
    void applyGCSVirtualHostedStyle();

    /// Set only by `fromDisk`: the settings of the disk that backs this configuration. They already
    /// carry the endpoint and the credentials, so `createObjectStorage` uses them as they are instead
    /// of translating the `s3(...)` argument grammar.
    std::shared_ptr<const GCSObjectStorageSettings> disk_settings;
    String backing_disk_name;
    /// The `google_adc_token_uri` key-value argument, which has no counterpart in `S3AuthSettings`.
    String google_adc_token_uri;
};

}

#endif
