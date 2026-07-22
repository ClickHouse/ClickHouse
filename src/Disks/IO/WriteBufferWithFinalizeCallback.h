#pragma once


#include <IO/WriteBufferFromFile.h>
#include <IO/WriteBufferFromFileDecorator.h>

namespace DB
{

using FinalizeCallback = std::function<void(size_t bytes_count)>;
/// Invoked to fsync the local metadata file that was written for this logical path, when the
/// caller requested durability (e.g. fsync_after_insert). No-op for metadata storages that are
/// not backed by a syncable local file.
using SyncMetadataCallback = std::function<void()>;

/// Stores data in S3/HDFS and adds the object path and object size to metadata file on local FS.
/// Optimization to avoid unnecessary request to objects storage: if do_not_write_empty_ is true and the file is empty it is not written.
/// NOTE: this optimizations depends on the disk type so the caller decides if it is safe to use it.
/// E.g. DiskS3 with metadata in local files and the data in ObjectStorage can have empty list of blobs in metadata file.
class WriteBufferWithFinalizeCallback final : public WriteBufferFromFileDecorator
{
public:
    WriteBufferWithFinalizeCallback(
        std::unique_ptr<WriteBuffer> impl_,
        FinalizeCallback && create_callback_,
        SyncMetadataCallback && sync_metadata_callback_,
        const String & remote_path_,
        bool create_blob_if_empty_);

    String getFileName() const override { return remote_path; }

    void preFinalize() override;

    /// Forward sync to the data buffer(s) and request a sync of the local metadata file. The
    /// request is latched because sync may arrive before or after the metadata is written (compact
    /// parts sync a stream before finalizing it, wide parts after).
    void sync() override;

private:
    void finalizeImpl() override;

    FinalizeCallback create_metadata_callback;
    SyncMetadataCallback sync_metadata_callback;
    String remote_path;
    const bool create_blob_if_empty = true;
    bool sync_requested = false;
    bool metadata_written = false;
};

}
