#pragma once

#if !defined(ARCADIA_BUILD)
#include <Common/config.h>
#endif

#if USE_AZURE_BLOB_STORAGE

#include <Disks/IDiskRemote.h>
#include <IO/ReadBufferFromBlobStorage.h>
#include <IO/WriteBufferFromBlobStorage.h>
#include <IO/SeekAvoidingReadBuffer.h>

#include <azure/identity/managed_identity_credential.hpp>
#include <azure/storage/blobs.hpp>


namespace DB
{

struct DiskBlobStorageSettings final
{
    DiskBlobStorageSettings(
        UInt64 max_single_part_upload_size_,
        UInt64 min_bytes_for_seek_,
        int max_single_read_retries,
        int max_single_download_retries,
        int thread_pool_size_,
        int objects_chunk_size_to_delete_);

    size_t max_single_part_upload_size; // NOTE: on 32-bit machines it will be at most 4GB, but size_t is also used in BufferBase for offset
    UInt64 min_bytes_for_seek;
    size_t max_single_read_retries;
    size_t max_single_download_retries;
    size_t thread_pool_size;
    size_t objects_chunk_size_to_delete;
};


class DiskBlobStorage final : public IDiskRemote
{
public:

    using SettingsPtr = std::unique_ptr<DiskBlobStorageSettings>;
    using GetDiskSettings = std::function<SettingsPtr(const Poco::Util::AbstractConfiguration &, const String, ContextPtr)>;

    DiskBlobStorage(
        const String & name_,
        DiskPtr metadata_disk_,
        std::shared_ptr<Azure::Storage::Blobs::BlobContainerClient> blob_container_client_,
        SettingsPtr settings_,
        GetDiskSettings settings_getter_);

    std::unique_ptr<ReadBufferFromFileBase> readFile(
        const String & path,
        const ReadSettings & settings,
        std::optional<size_t> estimated_size) const override;

    std::unique_ptr<WriteBufferFromFileBase> writeFile(
        const String & path,
        size_t buf_size,
        WriteMode mode) override;

    DiskType getType() const override;

    bool isRemote() const override;

    bool supportZeroCopyReplication() const override;

    bool checkUniqueId(const String & id) const override;

    void removeFromRemoteFS(RemoteFSPathKeeperPtr fs_paths_keeper) override;

    RemoteFSPathKeeperPtr createFSPathKeeper() const override;

    void applyNewSettings(const Poco::Util::AbstractConfiguration & config, ContextPtr context, const String &, const DisksMap &) override;

private:

    /// client used to access the files in the Blob Storage cloud
    std::shared_ptr<Azure::Storage::Blobs::BlobContainerClient> blob_container_client;

    MultiVersion<DiskBlobStorageSettings> current_settings;
    /// Gets disk settings from context.
    GetDiskSettings settings_getter;
};

}

#endif
