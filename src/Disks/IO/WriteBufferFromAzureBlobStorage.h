#pragma once

#include "config.h"

#if USE_AZURE_BLOB_STORAGE

#include <memory>
#include <mutex>

#include <base/defines.h>
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
        ThreadPoolCallbackRunnerUnsafe<void> schedule_ = {},
        AzureBlobStorage::ContainerClientRefreshCallback credentials_refresh_callback_ = {});

    ~WriteBufferFromAzureBlobStorage() override;

    void nextImpl() override;
    void preFinalize() override;
    std::string getFileName() const override { return blob_path; }
    void sync() override { next(); }

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
    /// `func` gets the client for its attempt: a credentials refresh replaces it, so it must not be captured outside.
    void execWithRetry(std::function<void(size_t, const AzureClientPtr &)> func, size_t num_tries, size_t cost = 0);
    void uploadBlock(const char * data, size_t size);

    AzureClientPtr getClient() const;

    /// On an auth failure, swap in a client rebuilt with refreshed credentials; returns true to retry.
    /// `used_client` is the client of the failed attempt, so that an attempt racing with a refresh
    /// done by another part upload retries with the fresh client instead of giving up.
    bool tryRefreshCredentials(const Azure::Core::RequestFailedException & e, const AzureClientPtr & used_client);

    /// Returns true if not a single byte was written to the buffer
    bool isEmpty() const { return total_size == 0 && count() == 0 && hidden_size == 0 && offset() == 0; }

    Azure::Core::Context azure_context;

    LoggerPtr log;
    LogSeriesLimiterPtr limited_log = std::make_shared<LogSeriesLimiter>(log, 1, 5);

    BufferAllocationPolicyPtr buffer_allocation_policy;

    const size_t max_single_part_upload_size;
    const size_t max_unexpected_write_error_retries;
    const std::string blob_path;
    const WriteSettings write_settings;

    /// Track that prefinalize() is called only once
    bool is_prefinalized = false;

    /// Part uploads run in parallel, so a refresh may replace the client while they are in flight.
    mutable std::mutex client_mutex;
    AzureClientPtr blob_container_client TSA_GUARDED_BY(client_mutex);
    bool credentials_refreshed TSA_GUARDED_BY(client_mutex) = false;

    const AzureBlobStorage::ContainerClientRefreshCallback credentials_refresh_callback;

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

    std::deque<PartData> detached_part_data;

    String container_for_logging;
    BlobStorageLogWriterPtr blob_log;
};

}

#endif
