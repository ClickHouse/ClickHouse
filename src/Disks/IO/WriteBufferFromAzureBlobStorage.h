#pragma once

#include "config.h"

#if USE_AZURE_BLOB_STORAGE

#include <memory>

#include <IO/WriteBufferFromFileBase.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteSettings.h>
#include <azure/storage/blobs.hpp>
#include <azure/core/io/body_stream.hpp>
#include <Common/ThreadPoolTaskTracker.h>
#include <Common/BufferAllocationPolicy.h>
#include <Common/BlobStorageLogWriter.h>
#include <Disks/DiskObjectStorage/ObjectStorages/AzureBlobStorage/AzureObjectStorage.h>

namespace Poco
{
class Logger;
}

namespace DB
{

class TaskTracker;

class WriteBufferFromAzureBlobStorage : public WriteBufferFromFileBase
{
public:
    using AzureClientPtr = std::shared_ptr<const AzureBlobStorage::ContainerClient>;

    WriteBufferFromAzureBlobStorage(
        AzureClientPtr blob_container_client_,
        const String & blob_path_,
        size_t buf_size_,
        const WriteSettings & write_settings_,
        std::shared_ptr<const AzureBlobStorage::RequestSettings> settings_,
        const String & container_for_logging_ = {},
        BlobStorageLogWriterPtr blob_log_ = {},
        ThreadPoolCallbackRunnerUnsafe<void> schedule_ = {});

    ~WriteBufferFromAzureBlobStorage() override;

    void nextImpl() override;
    void preFinalize() override;
    std::string getFileName() const override { return blob_path; }
    void sync() override { next(); }
    std::optional<WriteOutcome> getWriteOutcome() const override { return attempts_record.outcome(); }

private:
    struct PartData;

    void writeMultipartUpload();
    void writePart(PartData && part_data);
    void detachBuffer();
    void reallocateFirstBuffer();
    void allocateBuffer();
    void hidePartialData();
    void setFakeBufferWhenPreFinalized();

    void finalizeImpl() override;
    void execWithRetry(std::function<void(size_t)> func, size_t num_tries, size_t cost = 0);
    /// `execWithRetry` of a call that publishes the blob, which records the outcome. The transport records every attempt,
    /// also the retries of the SDK. After a failure that an applied attempt may explain, the idempotency id in the metadata
    /// of the blob proves the outcome. A write proven to be applied succeeds.
    void publishWithRetry(std::function<void(size_t)> func, size_t num_tries, size_t cost = 0);
    AttemptsRecord::Proof proveFailedWrite() const;

    /// Who wrote the blob, told by the idempotency id in its metadata.
    enum class BlobWriter : uint8_t
    {
        ThisWrite,
        AnotherWrite,
        /// A failed `Get Blob Properties`, or no blob.
        Unproven,
    };
    BlobWriter findBlobWriter() const;

    /// The conditions and the idempotency id of a call that publishes the blob.
    template <typename Options>
    void setPublishOptions(Options & options) const;
    void uploadBlock(const char * data, size_t size);

    /// Returns true if not a single byte was written to the buffer
    bool isEmpty() const { return total_size == 0 && count() == 0 && hidden_size == 0 && offset() == 0; }

    Azure::Core::Context azure_context;

    LoggerPtr log;
    LogSeriesLimiterPtr limited_log = std::make_shared<LogSeriesLimiter>(log, 1, 5);

    BufferAllocationPolicyPtr buffer_allocation_policy;

    const std::shared_ptr<const AzureBlobStorage::RequestSettings> settings;
    const size_t max_single_part_upload_size;
    const size_t max_unexpected_write_error_retries;
    const std::string blob_path;
    const WriteSettings write_settings;
    /// Stamped in the metadata of a conditional write. Empty for any other write.
    const String idempotency_id;

    /// Track that prefinalize() is called only once
    bool is_prefinalized = false;

    AzureClientPtr blob_container_client;
    std::vector<std::string> block_ids;

    using MemoryBufferPtr = std::unique_ptr<Memory<>>;
    MemoryBufferPtr tmp_buffer;
    size_t tmp_buffer_write_offset = 0;

    MemoryBufferPtr allocateBuffer() const;

    char fake_buffer_when_prefinalized[1] = {};

    bool first_buffer = true;

    size_t total_size = 0;
    size_t hidden_size = 0;

    std::unique_ptr<TaskTracker> task_tracker;
    bool check_objects_after_upload = false;

    /// Of the call that publishes the blob, on the thread of `finalize`.
    AttemptsRecord attempts_record;

    std::deque<PartData> detached_part_data;

    String container_for_logging;
    BlobStorageLogWriterPtr blob_log;
};

}

#endif
