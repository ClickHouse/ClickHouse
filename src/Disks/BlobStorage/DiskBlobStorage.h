#pragma once

#if !defined(ARCADIA_BUILD)
#include <Common/config.h>
#endif

#if USE_AZURE_BLOB_STORAGE

#include <Disks/IDiskRemote.h>
#include <IO/ReadBufferFromBlobStorage.h>
#include <IO/WriteBufferFromBlobStorage.h>
#include <Disks/ReadIndirectBufferFromRemoteFS.h>
#include <Disks/WriteIndirectBufferFromRemoteFS.h>
#include <IO/SeekAvoidingReadBuffer.h>

#include <azure/identity/managed_identity_credential.hpp>
#include <azure/storage/blobs.hpp>


namespace DB
{

void blob_do_sth();

class DiskBlobStorage final : public IDiskRemote
{
public:

    DiskBlobStorage(
        const String & name_,
        const String & remote_fs_root_path_,
        const String & metadata_path_,
        const String & log_name_,
        size_t thread_pool_size);

    DiskBlobStorage();

    DiskBlobStorage(
        const String & name_,
        const String & metadata_path_,
        const String & endpoint_url,
        std::shared_ptr<Azure::Identity::ManagedIdentityCredential> managed_identity_credential_,
        Azure::Storage::Blobs::BlobContainerClient blob_container_client_,
        size_t thread_pool_size_ = 1
    );

    std::unique_ptr<ReadBufferFromFileBase> readFile(
        const String &,
        size_t,
        size_t,
        size_t,
        size_t,
        MMappedFileCache *) const override;

    std::unique_ptr<WriteBufferFromFileBase> writeFile(
        const String &,
        size_t,
        WriteMode) override;

    DiskType::Type getType() const override;

    bool supportZeroCopyReplication() const override;

    bool checkUniqueId(const String &) const override;

    void removeFromRemoteFS(RemoteFSPathKeeperPtr) override;

    RemoteFSPathKeeperPtr createFSPathKeeper() const override;

private:

};

}

#endif
