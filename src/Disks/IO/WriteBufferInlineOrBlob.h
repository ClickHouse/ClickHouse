#pragma once

#include <IO/WriteBufferFromFileBase.h>

#include <functional>
#include <variant>

namespace DB
{

/// A blob of `bytes_count` bytes was uploaded to the object storage.
struct WrittenBlob { size_t bytes_count = 0; };
/// The whole content fits into the metadata itself; no blob was uploaded.
struct InlineData { String data; };
using FinalizeResult = std::variant<WrittenBlob, InlineData>;

using FinalizeCallback = std::function<void(FinalizeResult result)>;

/// Invoked to fsync the local metadata file that was written for this logical path, when the
/// caller requested durability (e.g. fsync_after_insert). No-op for metadata storages that are
/// not backed by a syncable local file.
using SyncMetadataCallback = std::function<void()>;

/// Writes a file as inline metadata content or as a blob and reports the outcome to a single
/// `finalize_callback`. Content up to `max_inline_bytes` finishes as `InlineData`; the first byte
/// past it builds the blob write stack (`create_underlying`) and the write finishes as
/// `WrittenBlob`. A threshold of 0 disables inlining; an empty write then uploads a blob only if
/// `create_blob_if_empty` is set, and still finishes as `WrittenBlob`.
class WriteBufferInlineOrBlob final : public WriteBufferFromFileBase
{
public:
    using CreateUnderlying = std::function<std::unique_ptr<WriteBuffer>()>;

    WriteBufferInlineOrBlob(
        String file_name_,
        size_t max_inline_bytes_,
        bool create_blob_if_empty_,
        CreateUnderlying create_underlying_,
        FinalizeCallback finalize_callback_,
        SyncMetadataCallback sync_metadata_callback_,
        size_t buf_size);

    void preFinalize() override;

    /// Sync the blob writers, if any, and request a sync of the local metadata file. The request is
    /// latched because sync may arrive before or after the metadata is written (compact parts sync a
    /// stream before finalizing it, wide parts after), and inline content has no blob to sync at all.
    void sync() override;

    std::string getFileName() const override { return file_name; }

private:
    void nextImpl() override;
    void finalizeImpl() override;
    void cancelImpl() noexcept override;

    void spill();

    const String file_name;
    const size_t max_inline_bytes;
    const bool create_blob_if_empty;
    const CreateUnderlying create_underlying;
    const FinalizeCallback finalize_callback;
    const SyncMetadataCallback sync_metadata_callback;

    String accumulated;
    std::unique_ptr<WriteBuffer> underlying;
    bool sync_requested = false;
    bool metadata_written = false;
};

}
