#pragma once

#include "config.h"

#if USE_AWS_S3

#include <base/types.h>
#include <Common/logger_useful.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteSettings.h>
#include <IO/S3Settings.h>
#include <Common/threadPoolCallbackRunner.h>
#include <IO/S3/BlobStorageLogWriter.h>
#include <Common/BufferAllocationPolicy.h>

#include <memory>
#include <vector>
#include <list>


namespace DB
{
/**
 * Buffer to write a data to a S3 object with specified bucket and key.
 * If data size written to the buffer is less than 'max_single_part_upload_size' write is performed using singlepart upload.
 * In another case multipart upload is used:
 * Data is divided on chunks with size greater than 'minimum_upload_part_size'. Last chunk can be less than this threshold.
 * Each chunk is written as a part to S3.
 */
class TaskTracker;

class WriteBufferFromS3 final : public WriteBufferFromFileBase
{
public:
    WriteBufferFromS3(
        std::shared_ptr<const S3::Client> client_ptr_,
        const String & bucket_,
        const String & key_,
        size_t buf_size_,
        const S3::S3RequestSettings & request_settings_,
        BlobStorageLogWriterPtr blob_log_,
        std::optional<std::map<String, String>> object_metadata_ = std::nullopt,
        ThreadPoolCallbackRunnerUnsafe<void> schedule_ = {},
        const WriteSettings & write_settings_ = {});

    ~WriteBufferFromS3() override;
    void nextImpl() override;
    void preFinalize() override;
    std::string getFileName() const override { return key; }
    void sync() override { next(); }

private:
    /// Receives response from the server after sending all data.
    void finalizeImpl() override;

    void cancelImpl() noexcept override;

    String getVerboseLogDetails() const;
    String getShortLogDetails() const;

    struct PartData;
    void hidePartialData();
    void reallocateFirstBuffer();
    void detachBuffer();
    void allocateBuffer();
    void setFakeBufferWhenPreFinalized();

    S3::UploadPartRequest getUploadRequest(size_t part_number, PartData & data);
    void writePart(PartData && data);
    void writeMultipartUpload();
    void createMultipartUpload();
    void completeMultipartUpload();
    void abortMultipartUpload();
    void tryToAbortMultipartUpload() noexcept;

    S3::PutObjectRequest getPutRequest(PartData & data);
    void makeSinglepartUpload(PartData && data);

    const String bucket;
    const String key;
    const S3::S3RequestSettings request_settings;
    const WriteSettings write_settings;
    const std::shared_ptr<const S3::Client> client_ptr;
    const std::optional<std::map<String, String>> object_metadata;
    LoggerPtr log = getLogger("WriteBufferFromS3");
    LogSeriesLimiterPtr limited_log = std::make_shared<LogSeriesLimiter>(log, 1, 5);

    BufferAllocationPolicyPtr buffer_allocation_policy;

    /// Upload in S3 is made in parts.
    /// We initiate upload, then upload each part and get ETag as a response, and then finalizeImpl() upload with listing all our parts.
    String multipart_upload_id;
    std::deque<String> multipart_tags;
    std::deque<String> multipart_checksums; // if enabled
    bool multipart_upload_finished = false;

    /// Track that prefinalize() is called only once
    bool is_prefinalized = false;

    /// First fully filled buffer has to be delayed
    /// There are two ways after:
    /// First is to call prefinalize/finalize, which leads to single part upload
    /// Second is to write more data, which leads to multi part upload
    std::deque<PartData> detached_part_data;
    char fake_buffer_when_prefinalized[1] = {};

    /// offset() and count() are unstable inside nextImpl
    /// For example nextImpl changes position hence offset() and count() is changed
    /// This vars are dedicated to store information about sizes when offset() and count() are unstable
    size_t total_size = 0;
    size_t hidden_size = 0;

    std::unique_ptr<TaskTracker> task_tracker;

    BlobStorageLogWriterPtr blob_log;
};

}

#endif
