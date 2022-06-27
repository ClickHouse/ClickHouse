#pragma once

#include <Common/config.h>

#if USE_AZURE_BLOB_STORAGE

#include <Disks/IDiskRemote.h>
#include <IO/ReadBufferFromAzureBlobStorage.h>
#include <IO/WriteBufferFromAzureBlobStorage.h>
#include <IO/SeekAvoidingReadBuffer.h>

#include <azure/identity/managed_identity_credential.hpp>
#include <azure/storage/blobs.hpp>


namespace DB
{

struct DiskAzureBlobStorageSettings final
{
    DiskAzureBlobStorageSettings(
        UInt64 max_single_part_upload_size_,
        UInt64 min_bytes_for_seek_,
        int max_single_read_retries,
        int max_single_download_retries,
        int thread_pool_size_);

    size_t max_single_part_upload_size; /// NOTE: on 32-bit machines it will be at most 4GB, but size_t is also used in BufferBase for offset
    UInt64 min_bytes_for_seek;
    size_t max_single_read_retries;
    size_t max_single_download_retries;
    size_t thread_pool_size;
};


class DiskAzureBlobStorage final : public IDiskRemote
{
public:

    using SettingsPtr = std::unique_ptr<DiskAzureBlobStorageSettings>;
    using GetDiskSettings = std::function<SettingsPtr(const Poco::Util::AbstractConfiguration &, const String, ContextPtr)>;

    DiskAzureBlobStorage(
        const String & name_,
        DiskPtr metadata_disk_,
        std::shared_ptr<Azure::Storage::Blobs::BlobContainerClient> blob_container_client_,
        SettingsPtr settings_,
        GetDiskSettings settings_getter_);

    std::unique_ptr<ReadBufferFromFileBase> readFile(
        const String & path,
        const ReadSettings & settings,
        std::optional<size_t> read_hint,
        std::optional<size_t> file_size) const override;

    std::unique_ptr<WriteBufferFromFileBase> writeFile(
        const String & path,
        size_t buf_size,
        WriteMode mode,
        const WriteSettings & settings) override;

    DiskType getType() const override;

    bool isRemote() const override;

    bool supportZeroCopyReplication() const override;

    bool checkUniqueId(const String & id) const override;

    void removeFromRemoteFS(const std::vector<String> & paths) override;

    void applyNewSettings(const Poco::Util::AbstractConfiguration & config, ContextPtr context, const String &, const DisksMap &) override;

private:

    /// client used to access the files in the Blob Storage cloud
    std::shared_ptr<Azure::Storage::Blobs::BlobContainerClient> blob_container_client;

    MultiVersion<DiskAzureBlobStorageSettings> current_settings;
    /// Gets disk settings from context.
    GetDiskSettings settings_getter;
};

}

#endif
