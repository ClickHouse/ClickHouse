#pragma once

#include "config.h"

#if USE_GOOGLE_CLOUD

#include <memory>
#include <optional>
#include <base/types.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/WriteSettings.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Common/BlobStorageLogWriter.h>
#include <Common/logger_useful.h>

#include <google/cloud/storage/client.h>

namespace DB
{

/// Writes a GCS object through the native google-cloud-cpp storage client.
///
/// Backed by a `google::cloud::storage::ObjectWriteStream` (a std::ostream). The SDK transparently
/// switches to a resumable upload for large objects, so no explicit multipart handling is needed.
/// Only whole-object rewrites are supported (GCS objects are immutable; there is no append).
class WriteBufferFromGCS final : public WriteBufferFromFileBase
{
public:
    WriteBufferFromGCS(
        std::shared_ptr<google::cloud::storage::Client> client_,
        const String & bucket_,
        const String & key_,
        size_t buf_size_,
        const WriteSettings & write_settings_,
        BlobStorageLogWriterPtr blob_log_,
        std::optional<ObjectAttributes> attributes_ = std::nullopt,
        bool for_disk_ = false);

    ~WriteBufferFromGCS() override;

    void nextImpl() override;
    void sync() override { next(); }
    std::string getFileName() const override { return key; }

private:
    void finalizeImpl() override;
    void cancelImpl() noexcept override;
    void logUploadResult(Int32 error_code, const String & error_message);

    std::shared_ptr<google::cloud::storage::Client> client;
    const String bucket;
    const String key;
    const WriteSettings write_settings;
    BlobStorageLogWriterPtr blob_log;
    const std::optional<ObjectAttributes> attributes;
    /// Attributes request counters to `DiskGCS*` in addition to `GCS*`.
    const bool for_disk;

    std::unique_ptr<google::cloud::storage::ObjectWriteStream> write_stream;

    size_t total_bytes_written = 0;
    UInt64 total_time_microseconds = 0;
    bool upload_result_logged = false;

    LoggerPtr log = getLogger("WriteBufferFromGCS");
};

}

#endif
