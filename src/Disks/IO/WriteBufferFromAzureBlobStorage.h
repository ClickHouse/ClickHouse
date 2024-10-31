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
#include <Disks/ObjectStorages/AzureBlobStorage/AzureObjectStorage.h>

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
    using AzureClientPtr = std::shared_ptr<const Azure::Storage::Blobs::BlobContainerClient>;

    WriteBufferFromAzureBlobStorage(
        AzureClientPtr blob_container_client_,
        const String & blob_path_,
        size_t buf_size_,
        const WriteSettings & write_settings_,
        std::shared_ptr<const AzureBlobStorage::RequestSettings> settings_,
        ThreadPoolCallbackRunnerUnsafe<void> schedule_ = {});

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
    void execWithRetry(std::function<void()> func, size_t num_tries, size_t cost = 0);
    void uploadBlock(const char * data, size_t size);

    LoggerPtr log;
    LogSeriesLimiterPtr limited_log = std::make_shared<LogSeriesLimiter>(log, 1, 5);

    BufferAllocationPolicyPtr buffer_allocation_policy;

    const size_t max_single_part_upload_size;
    const size_t max_unexpected_write_error_retries;
    const std::string blob_path;
    const WriteSettings write_settings;

    /// Track that prefinalize() is called only once
    bool is_prefinalized = false;

    AzureClientPtr blob_container_client;
    std::vector<std::string> block_ids;

    using MemoryBufferPtr = std::unique_ptr<Memory<>>;
    MemoryBufferPtr tmp_buffer;
    size_t tmp_buffer_write_offset = 0;

    MemoryBufferPtr allocateBuffer() const;

    char fake_buffer_when_prefinalized[1] = {};

    bool first_buffer=true;

    size_t total_size = 0;
    size_t hidden_size = 0;

    std::unique_ptr<TaskTracker> task_tracker;
    bool check_objects_after_upload = false;

    std::deque<PartData> detached_part_data;
};

}

#endif
