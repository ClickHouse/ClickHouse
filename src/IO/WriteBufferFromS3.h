#pragma once

#include <Common/config.h>

#if USE_AWS_S3

#include <memory>
#include <vector>
#include <list>
#include <base/logger_useful.h>
#include <base/types.h>

#include <Common/ThreadPool.h>
#include <Common/FileCache_fwd.h>
#include <Common/FileSegment.h>

#include <IO/BufferWithOwnMemory.h>
#include <IO/WriteBuffer.h>
#include <Storages/StorageS3Settings.h>

#include <aws/core/utils/memory/stl/AWSStringStream.h>

namespace Aws::S3
{
class S3Client;
}

namespace Aws::S3::Model
{
    class UploadPartRequest;
    class PutObjectRequest;
}

namespace DB
{

using ScheduleFunc = std::function<void(std::function<void()>)>;
class WriteBufferFromFile;

/**
 * Buffer to write a data to a S3 object with specified bucket and key.
 * If data size written to the buffer is less than 'max_single_part_upload_size' write is performed using singlepart upload.
 * In another case multipart upload is used:
 * Data is divided on chunks with size greater than 'minimum_upload_part_size'. Last chunk can be less than this threshold.
 * Each chunk is written as a part to S3.
 */
class WriteBufferFromS3 final : public BufferWithOwnMemory<WriteBuffer>
{
public:
    explicit WriteBufferFromS3(
        std::shared_ptr<Aws::S3::S3Client> client_ptr_,
        const String & bucket_,
        const String & key_,
        const S3Settings::ReadWriteSettings & s3_settings_,
        std::optional<std::map<String, String>> object_metadata_ = std::nullopt,
        size_t buffer_size_ = DBMS_DEFAULT_BUFFER_SIZE,
        ScheduleFunc schedule_ = {},
        FileCachePtr cache_ = nullptr);

    ~WriteBufferFromS3() override;

    void nextImpl() override;

    void preFinalize() override;

private:
    void allocateBuffer();

    void createMultipartUpload();
    void writePart();
    void completeMultipartUpload();

    void makeSinglepartUpload();

    /// Receives response from the server after sending all data.
    void finalizeImpl() override;

    struct UploadPartTask;
    void fillUploadRequest(Aws::S3::Model::UploadPartRequest & req, int part_number);
    void processUploadRequest(UploadPartTask & task);

    struct PutObjectTask;
    void fillPutRequest(Aws::S3::Model::PutObjectRequest & req);
    void processPutRequest(PutObjectTask & task);

    void waitForReadyBackGroundTasks();
    void waitForAllBackGroundTasks();

    bool cacheEnabled() const;

    String bucket;
    String key;
    std::optional<std::map<String, String>> object_metadata;
    std::shared_ptr<Aws::S3::S3Client> client_ptr;
    size_t upload_part_size = 0;
    S3Settings::ReadWriteSettings s3_settings;
    /// Buffer to accumulate data.
    std::shared_ptr<Aws::StringStream> temporary_buffer;
    size_t last_part_size = 0;
    std::atomic<size_t> total_parts_uploaded = 0;

    /// Upload in S3 is made in parts.
    /// We initiate upload, then upload each part and get ETag as a response, and then finalizeImpl() upload with listing all our parts.
    String multipart_upload_id;
    std::vector<String> part_tags;

    bool is_prefinalized = false;

    /// Following fields are for background uploads in thread pool (if specified).
    /// We use std::function to avoid dependency of Interpreters
    ScheduleFunc schedule;
    std::unique_ptr<PutObjectTask> put_object_task;
    std::list<UploadPartTask> upload_object_tasks;
    size_t num_added_bg_tasks = 0;
    size_t num_finished_bg_tasks = 0;
    std::mutex bg_tasks_mutex;
    std::condition_variable bg_tasks_condvar;

    Poco::Logger * log = &Poco::Logger::get("WriteBufferFromS3");

    FileCachePtr cache;
    size_t current_download_offset = 0;
    std::optional<FileSegmentsHolder> file_segments_holder;
    static void finalizeCacheIfNeeded(std::optional<FileSegmentsHolder> &);
};

}

#endif
