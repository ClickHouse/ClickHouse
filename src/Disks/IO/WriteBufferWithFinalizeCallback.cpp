#include <Disks/IO/WriteBufferWithFinalizeCallback.h>

namespace DB
{

WriteBufferWithFinalizeCallback::WriteBufferWithFinalizeCallback(
    std::unique_ptr<WriteBuffer> impl_,
    FinalizeCallback && create_callback_,
    SyncMetadataCallback && sync_metadata_callback_,
    const String & remote_path_,
    bool create_blob_if_empty_)
    : WriteBufferFromFileDecorator(std::move(impl_))
    , create_metadata_callback(std::move(create_callback_))
    , sync_metadata_callback(std::move(sync_metadata_callback_))
    , remote_path(remote_path_)
    , create_blob_if_empty(create_blob_if_empty_)
{
}

void WriteBufferWithFinalizeCallback::preFinalize()
{
    /// Underlying buffer can initiate write to the storage in its preFinalize.
    /// So we skip this call if the buffer is empty and do_not_write_empty optimization is enabled.
    const auto bytes_written = count();
    if (create_blob_if_empty || bytes_written > 0)
        WriteBufferFromFileDecorator::preFinalize();
}

void WriteBufferWithFinalizeCallback::finalizeImpl()
{
    const auto bytes_written = count();
    if (create_blob_if_empty || bytes_written > 0)
        WriteBufferFromFileDecorator::finalizeImpl();
    else
        /// If the buffer is empty and create_blob_if_empty optimization is disabled we cancel the write.
        /// This will call cancelImpl of the underlying buffer.
        WriteBufferFromFileDecorator::cancel();

    if (create_metadata_callback)
        create_metadata_callback(bytes_written);

    /// Metadata is now recorded; honor a sync that arrived before finalize (compact parts).
    metadata_written = true;
    if (sync_requested && sync_metadata_callback)
        sync_metadata_callback();
}

void WriteBufferWithFinalizeCallback::sync()
{
    /// Sync the data buffer(s): ForkWriteBuffer forwards to the underlying writers (a real fdatasync
    /// for local-file-backed object storage, a no-op for s3/azure whose durability is on finalize).
    WriteBufferFromFileDecorator::sync();

    /// Latch and request the metadata-file sync; honored here or by finalizeImpl once it exists.
    sync_requested = true;
    if (metadata_written && sync_metadata_callback)
        sync_metadata_callback();
}


}
