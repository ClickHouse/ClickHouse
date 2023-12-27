#include <IO/AzureBlobStorage/copyAzureBlobStorageFile.h>

#if USE_AZURE_BLOB_STORAGE

#include <Common/ProfileEvents.h>
#include <Common/typeid_cast.h>
#include <Interpreters/Context.h>
#include <IO/LimitSeekableReadBuffer.h>
#include <IO/SeekableReadBuffer.h>
#include <IO/StdStreamFromReadBuffer.h>
#include <Disks/IO/ReadBufferFromAzureBlobStorage.h>
#include <Disks/IO/WriteBufferFromAzureBlobStorage.h>
#include <Common/getRandomASCIIString.h>
#include <IO/SharedThreadPools.h>

namespace ProfileEvents
{
    extern const Event AzureCopyObject;
    extern const Event AzureUploadPart;

    extern const Event DiskAzureCopyObject;
    extern const Event DiskAzureUploadPart;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


namespace DB
{

size_t max_single_operation_copy_size = 256 * 1024 * 1024;


namespace
{
    class UploadHelper
    {
    public:
        UploadHelper(
            const CreateReadBuffer & create_read_buffer_,
            std::shared_ptr<Azure::Storage::Blobs::BlobContainerClient> client_,
            size_t offset_,
            size_t total_size_,
            const String & dest_bucket_,
            const String & dest_key_,
            std::shared_ptr<AzureObjectStorageSettings> settings_,
            const std::optional<std::map<String, String>> & object_metadata_,
            ThreadPoolCallbackRunner<void> schedule_,
            bool for_disk_azure_blob_storage_,
            const Poco::Logger * log_)
            : create_read_buffer(create_read_buffer_)
            , client(client_)
            , offset (offset_)
            , total_size (total_size_)
            , dest_bucket(dest_bucket_)
            , dest_key(dest_key_)
            , settings(settings_)
            , object_metadata(object_metadata_)
            , schedule(schedule_)
            , for_disk_azure_blob_storage(for_disk_azure_blob_storage_)
            , log(log_)
            , max_single_part_upload_size(settings_.get()->max_single_part_upload_size)
        {
        }

        ~UploadHelper() {}

    protected:
        std::function<std::unique_ptr<SeekableReadBuffer>()> create_read_buffer;
        std::shared_ptr<Azure::Storage::Blobs::BlobContainerClient> client;
        size_t offset;
        size_t total_size;
        const String & dest_bucket;
        const String & dest_key;
        std::shared_ptr<AzureObjectStorageSettings> settings;
        const std::optional<std::map<String, String>> & object_metadata;
        ThreadPoolCallbackRunner<void> schedule;
        bool for_disk_azure_blob_storage;
        const Poco::Logger * log;
        size_t max_single_part_upload_size;

        struct UploadPartTask
        {
            char *data = nullptr;
            size_t size = 0;
            std::string block_id;
            bool is_finished = false;
            std::exception_ptr exception;

            ~UploadPartTask()
            {
                if (data != nullptr)
                    free(data);
            }
        };

        size_t normal_part_size;
        std::vector<std::string> block_ids;

        std::list<UploadPartTask> TSA_GUARDED_BY(bg_tasks_mutex) bg_tasks;
        int num_added_bg_tasks TSA_GUARDED_BY(bg_tasks_mutex) = 0;
        int num_finished_bg_tasks TSA_GUARDED_BY(bg_tasks_mutex) = 0;
        std::mutex bg_tasks_mutex;
        std::condition_variable bg_tasks_condvar;

    public:
        void performCopy()
        {
            performMultipartUpload();
        }

        void completeMultipartUpload()
        {
            auto block_blob_client = client->GetBlockBlobClient(dest_key);
            block_blob_client.CommitBlockList(block_ids);
        }

        void performMultipartUpload()
        {
            normal_part_size = 1024;

            size_t position = offset;
            size_t end_position = offset + total_size;

            try
            {
                while (position < end_position)
                {
                    size_t next_position = std::min(position + normal_part_size, end_position);
                    size_t part_size = next_position - position; /// `part_size` is either `normal_part_size` or smaller if it's the final part.

                    uploadPart(position, part_size);

                    position = next_position;
                }
            }
            catch (...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__);
                waitForAllBackgroundTasks();
                throw;
            }

            waitForAllBackgroundTasks();
            completeMultipartUpload();
        }


        void uploadPart(size_t part_offset, size_t part_size)
        {
            LOG_TRACE(log, "Writing part. Bucket: {}, Key: {}, Size: {}", dest_bucket, dest_key, part_size);

            if (!part_size)
            {
                LOG_TRACE(log, "Skipping writing an empty part.");
                return;
            }

            if (schedule)
            {
                UploadPartTask *  task = nullptr;

                {
                    std::lock_guard lock(bg_tasks_mutex);
                    task = &bg_tasks.emplace_back();
                    ++num_added_bg_tasks;
                }

                /// Notify waiting thread when task finished
                auto task_finish_notify = [this, task]()
                {
                    std::lock_guard lock(bg_tasks_mutex);
                    task->is_finished = true;
                    ++num_finished_bg_tasks;

                    /// Notification under mutex is important here.
                    /// Otherwise, WriteBuffer could be destroyed in between
                    /// Releasing lock and condvar notification.
                    bg_tasks_condvar.notify_one();
                };

                try
                {
                    auto read_buffer = std::make_unique<LimitSeekableReadBuffer>(create_read_buffer(), part_offset, part_size);
                    task->data = new char[part_size];
                    task->size = part_size;
                    size_t n = read_buffer->read(task->data,part_size);
                    if (n != part_size)
                        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected size");

                    schedule([this, task, task_finish_notify]()
                    {
                        try
                        {
                            processUploadTask(*task);
                        }
                        catch (...)
                        {
                            task->exception = std::current_exception();
                        }
                        task_finish_notify();
                    }, Priority{});
                }
                catch (...)
                {
                    task_finish_notify();
                    throw;
                }
            }
            else
            {
                UploadPartTask task;
                auto read_buffer = std::make_unique<LimitSeekableReadBuffer>(create_read_buffer(), part_offset, part_size);
                task.data = new char[part_size];
                size_t n = read_buffer->read(task.data,part_size);
                if (n != part_size)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected size");
                task.size = part_size;
                processUploadTask(task);
                block_ids.emplace_back(task.block_id);
            }
        }

        void processUploadTask(UploadPartTask & task)
        {
            auto block_id = processUploadPartRequest(task);

            std::lock_guard lock(bg_tasks_mutex); /// Protect bg_tasks from race
            task.block_id = block_id;
            LOG_TRACE(log, "Writing part finished. Bucket: {}, Key: {}, block_id: {}, Parts: {}", dest_bucket, dest_key, block_id, bg_tasks.size());
        }

        String processUploadPartRequest(UploadPartTask & task)
        {
            ProfileEvents::increment(ProfileEvents::AzureUploadPart);
            if (for_disk_azure_blob_storage)
                ProfileEvents::increment(ProfileEvents::DiskAzureUploadPart);

            auto block_blob_client = client->GetBlockBlobClient(dest_key);
            task.block_id = getRandomASCIIString(64);
            Azure::Core::IO::MemoryBodyStream memory(reinterpret_cast<const uint8_t *>(task.data), task.size);
            block_blob_client.StageBlock(task.block_id, memory);

            return task.block_id;
        }


        void waitForAllBackgroundTasks()
        {
            if (!schedule)
                return;

            std::unique_lock lock(bg_tasks_mutex);
            /// Suppress warnings because bg_tasks_mutex is actually hold, but tsa annotations do not understand std::unique_lock
            bg_tasks_condvar.wait(lock, [this]() {return TSA_SUPPRESS_WARNING_FOR_READ(num_added_bg_tasks) == TSA_SUPPRESS_WARNING_FOR_READ(num_finished_bg_tasks); });

            auto & tasks = TSA_SUPPRESS_WARNING_FOR_WRITE(bg_tasks);
            for (auto & task : tasks)
            {
                if (task.exception)
                    std::rethrow_exception(task.exception);
                block_ids.emplace_back(task.block_id);
            }
        }
    };
}


void copyDataToAzureBlobStorageFile(
    const std::function<std::unique_ptr<SeekableReadBuffer>()> & create_read_buffer,
    size_t offset,
    size_t size,
    std::shared_ptr<Azure::Storage::Blobs::BlobContainerClient> & dest_client,
    const String & dest_bucket,
    const String & dest_key,
    std::shared_ptr<AzureObjectStorageSettings> settings,
    const std::optional<std::map<String, String>> & object_metadata,
    ThreadPoolCallbackRunner<void> schedule,
    bool for_disk_azure_blob_storage)
{
    UploadHelper helper{create_read_buffer, dest_client, offset, size, dest_bucket, dest_key, settings, object_metadata, schedule, for_disk_azure_blob_storage, &Poco::Logger::get("copyDataToAzureBlobStorageFile")};
    helper.performCopy();
}


void copyAzureBlobStorageFile(
    std::shared_ptr<Azure::Storage::Blobs::BlobContainerClient> src_client,
    std::shared_ptr<Azure::Storage::Blobs::BlobContainerClient> dest_client,
    const String & src_bucket,
    const String & src_key,
    size_t offset,
    size_t size,
    const String & dest_bucket,
    const String & dest_key,
    std::shared_ptr<AzureObjectStorageSettings> settings,
    const ReadSettings & read_settings,
    const std::optional<std::map<String, String>> & object_metadata,
    ThreadPoolCallbackRunner<void> schedule,
    bool for_disk_azure_blob_storage)
{

    if (size < max_single_operation_copy_size)
    {
        ProfileEvents::increment(ProfileEvents::AzureCopyObject);
        if (for_disk_azure_blob_storage)
            ProfileEvents::increment(ProfileEvents::DiskAzureCopyObject);
        auto block_blob_client_src = src_client->GetBlockBlobClient(src_key);
        auto block_blob_client_dest = dest_client->GetBlockBlobClient(dest_key);
        auto uri = block_blob_client_src.GetUrl();
        block_blob_client_dest.CopyFromUri(uri);
    }
    else
    {
        LOG_TRACE(&Poco::Logger::get("copyAzureBlobStorageFile"), "Reading from Bucket: {}, Key: {}", src_bucket, src_key);
        auto create_read_buffer = [&]
        {
            return std::make_unique<ReadBufferFromAzureBlobStorage>(src_client, src_key, read_settings, settings->max_single_read_retries,
            settings->max_single_download_retries);
        };

        UploadHelper helper{create_read_buffer, dest_client, offset, size, dest_bucket, dest_key, settings, object_metadata, schedule, for_disk_azure_blob_storage, &Poco::Logger::get("copyAzureBlobStorageFile")};
        helper.performCopy();
    }
}

}

#endif
