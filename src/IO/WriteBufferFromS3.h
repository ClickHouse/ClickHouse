#pragma once

#include <Common/config.h>

#if USE_AWS_S3

#include <memory>
#include <vector>
#include <list>

#include <base/types.h>
#include <Common/logger_useful.h>
#include <Common/ThreadPool.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteSettings.h>
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
    WriteBufferFromS3(
        std::shared_ptr<const Aws::S3::S3Client> client_ptr_,
        const String & bucket_,
        const String & key_,
        const S3Settings::ReadWriteSettings & s3_settings_,
        std::optional<std::map<String, String>> object_metadata_ = std::nullopt,
        size_t buffer_size_ = DBMS_DEFAULT_BUFFER_SIZE,
        ScheduleFunc schedule_ = {},
        const WriteSettings & write_settings_ = {});

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
    void processPutRequest(const PutObjectTask & task);

    void waitForReadyBackGroundTasks();
    void waitForAllBackGroundTasks();
    void waitForAllBackGroundTasksUnlocked(std::unique_lock<std::mutex> & bg_tasks_lock);

    const String bucket;
    const String key;
    const S3Settings::ReadWriteSettings s3_settings;
    const std::shared_ptr<const Aws::S3::S3Client> client_ptr;
    const std::optional<std::map<String, String>> object_metadata;

    size_t upload_part_size = 0;
    std::shared_ptr<Aws::StringStream> temporary_buffer; /// Buffer to accumulate data.
    size_t last_part_size = 0;
    std::atomic<size_t> total_parts_uploaded = 0;

    /// Upload in S3 is made in parts.
    /// We initiate upload, then upload each part and get ETag as a response, and then finalizeImpl() upload with listing all our parts.
    String multipart_upload_id;
    std::vector<String> TSA_GUARDED_BY(bg_tasks_mutex) part_tags;

    bool is_prefinalized = false;

    /// Following fields are for background uploads in thread pool (if specified).
    /// We use std::function to avoid dependency of Interpreters
    const ScheduleFunc schedule;

    std::unique_ptr<PutObjectTask> put_object_task; /// Does not need protection by mutex because of the logic around is_finished field.
    std::list<UploadPartTask> TSA_GUARDED_BY(bg_tasks_mutex) upload_object_tasks;
    size_t num_added_bg_tasks TSA_GUARDED_BY(bg_tasks_mutex) = 0;
    size_t num_finished_bg_tasks TSA_GUARDED_BY(bg_tasks_mutex) = 0;

    std::mutex bg_tasks_mutex;
    std::condition_variable bg_tasks_condvar;

    Poco::Logger * log = &Poco::Logger::get("WriteBufferFromS3");

    WriteSettings write_settings;
};

}

#endif
