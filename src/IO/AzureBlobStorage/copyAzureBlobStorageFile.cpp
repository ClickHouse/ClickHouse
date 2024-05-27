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


namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_CONFIG_PARAMETER;
    extern const int AZURE_BLOB_STORAGE_ERROR;
}

namespace
{
    class UploadHelper
    {
    public:
        UploadHelper(
            const CreateReadBuffer & create_read_buffer_,
            std::shared_ptr<const Azure::Storage::Blobs::BlobContainerClient> client_,
            size_t offset_,
            size_t total_size_,
            const String & dest_container_for_logging_,
            const String & dest_blob_,
            std::shared_ptr<const AzureObjectStorageSettings> settings_,
            ThreadPoolCallbackRunnerUnsafe<void> schedule_,
            bool for_disk_azure_blob_storage_,
            const Poco::Logger * log_)
            : create_read_buffer(create_read_buffer_)
            , client(client_)
            , offset (offset_)
            , total_size (total_size_)
            , dest_container_for_logging(dest_container_for_logging_)
            , dest_blob(dest_blob_)
            , settings(settings_)
            , schedule(schedule_)
            , for_disk_azure_blob_storage(for_disk_azure_blob_storage_)
            , log(log_)
            , max_single_part_upload_size(settings_->max_single_part_upload_size)
        {
        }

        virtual ~UploadHelper() = default;

    protected:
        std::function<std::unique_ptr<SeekableReadBuffer>()> create_read_buffer;
        std::shared_ptr<const Azure::Storage::Blobs::BlobContainerClient> client;
        size_t offset;
        size_t total_size;
        const String & dest_container_for_logging;
        const String & dest_blob;
        std::shared_ptr<const AzureObjectStorageSettings> settings;
        ThreadPoolCallbackRunnerUnsafe<void> schedule;
        bool for_disk_azure_blob_storage;
        const Poco::Logger * log;
        size_t max_single_part_upload_size;

        struct UploadPartTask
        {
            size_t part_offset;
            size_t part_size;
            std::vector<std::string> block_ids;
            bool is_finished = false;
            std::exception_ptr exception;
        };

        size_t normal_part_size;
        std::vector<std::string> block_ids;

        std::list<UploadPartTask> TSA_GUARDED_BY(bg_tasks_mutex) bg_tasks;
        int num_added_bg_tasks TSA_GUARDED_BY(bg_tasks_mutex) = 0;
        int num_finished_bg_tasks TSA_GUARDED_BY(bg_tasks_mutex) = 0;
        std::mutex bg_tasks_mutex;
        std::condition_variable bg_tasks_condvar;

        void calculatePartSize()
        {
            auto max_upload_part_size = settings->max_upload_part_size;
            if (!max_upload_part_size)
                throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "max_upload_part_size must not be 0");
            /// We've calculated the size of a normal part (the final part can be smaller).
            normal_part_size = max_upload_part_size;
        }

    public:
        void performCopy()
        {
            performMultipartUpload();
        }

        void completeMultipartUpload()
        {
            auto block_blob_client = client->GetBlockBlobClient(dest_blob);
            block_blob_client.CommitBlockList(block_ids);
        }

        void performMultipartUpload()
        {
            calculatePartSize();

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
            LOG_TRACE(log, "Writing part. Container: {}, Blob: {}, Size: {}", dest_container_for_logging, dest_blob, part_size);

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
                    task->part_offset = part_offset;
                    task->part_size = part_size;

                    schedule([this, task, task_finish_notify]()
                    {
                        try
                        {
                            processUploadPartRequest(*task);
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
                task.part_offset = part_offset;
                task.part_size = part_size;
                processUploadPartRequest(task);
                block_ids.insert(block_ids.end(),task.block_ids.begin(), task.block_ids.end());
            }
        }

        void processUploadPartRequest(UploadPartTask & task)
        {
            ProfileEvents::increment(ProfileEvents::AzureUploadPart);
            if (for_disk_azure_blob_storage)
                ProfileEvents::increment(ProfileEvents::DiskAzureUploadPart);

            auto block_blob_client = client->GetBlockBlobClient(dest_blob);
            auto read_buffer = std::make_unique<LimitSeekableReadBuffer>(create_read_buffer(), task.part_offset, task.part_size);
            while (!read_buffer->eof())
            {
                  auto size = read_buffer->available();
                  if (size > 0)
                  {
                      auto block_id = getRandomASCIIString(64);
                      Azure::Core::IO::MemoryBodyStream memory(reinterpret_cast<const uint8_t *>(read_buffer->position()), size);
                      block_blob_client.StageBlock(block_id, memory);
                      task.block_ids.emplace_back(block_id);
                      read_buffer->ignore(size);
                      LOG_TRACE(log, "Writing part. Container: {}, Blob: {}, block_id: {}", dest_container_for_logging, dest_blob, block_id);
                  }
            }
            std::lock_guard lock(bg_tasks_mutex); /// Protect bg_tasks from race
            LOG_TRACE(log, "Writing part finished. Container: {}, Blob: {}, Parts: {}", dest_container_for_logging, dest_blob, bg_tasks.size());
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
                block_ids.insert(block_ids.end(),task.block_ids.begin(), task.block_ids.end());
            }
        }
    };
}


void copyDataToAzureBlobStorageFile(
    const std::function<std::unique_ptr<SeekableReadBuffer>()> & create_read_buffer,
    size_t offset,
    size_t size,
    std::shared_ptr<const Azure::Storage::Blobs::BlobContainerClient> dest_client,
    const String & dest_container_for_logging,
    const String & dest_blob,
    std::shared_ptr<const AzureObjectStorageSettings> settings,
    ThreadPoolCallbackRunnerUnsafe<void> schedule,
    bool for_disk_azure_blob_storage)
{
    UploadHelper helper{create_read_buffer, dest_client, offset, size, dest_container_for_logging, dest_blob, settings, schedule, for_disk_azure_blob_storage, &Poco::Logger::get("copyDataToAzureBlobStorageFile")};
    helper.performCopy();
}


void copyAzureBlobStorageFile(
    std::shared_ptr<const Azure::Storage::Blobs::BlobContainerClient> src_client,
    std::shared_ptr<const Azure::Storage::Blobs::BlobContainerClient> dest_client,
    const String & src_container_for_logging,
    const String & src_blob,
    size_t offset,
    size_t size,
    const String & dest_container_for_logging,
    const String & dest_blob,
    std::shared_ptr<const AzureObjectStorageSettings> settings,
    const ReadSettings & read_settings,
    ThreadPoolCallbackRunnerUnsafe<void> schedule,
    bool for_disk_azure_blob_storage)
{

    if (settings->use_native_copy)
    {
        ProfileEvents::increment(ProfileEvents::AzureCopyObject);
        if (for_disk_azure_blob_storage)
            ProfileEvents::increment(ProfileEvents::DiskAzureCopyObject);

        auto block_blob_client_src = src_client->GetBlockBlobClient(src_blob);
        auto block_blob_client_dest = dest_client->GetBlockBlobClient(dest_blob);
        auto source_uri = block_blob_client_src.GetUrl();

        if (size < settings->max_single_part_copy_size)
        {
            block_blob_client_dest.CopyFromUri(source_uri);
        }
        else
        {
            Azure::Storage::Blobs::StartBlobCopyOperation operation = block_blob_client_dest.StartCopyFromUri(source_uri);

            // Wait for the operation to finish, checking for status every 100 second.
            auto copy_response = operation.PollUntilDone(std::chrono::milliseconds(100));
            auto properties_model = copy_response.Value;

            if (properties_model.CopySource.HasValue())
            {
                throw Exception(ErrorCodes::AZURE_BLOB_STORAGE_ERROR, "Copy failed");
            }

        }
    }
    else
    {
        LOG_TRACE(&Poco::Logger::get("copyAzureBlobStorageFile"), "Reading from Container: {}, Blob: {}", src_container_for_logging, src_blob);
        auto create_read_buffer = [&]
        {
            return std::make_unique<ReadBufferFromAzureBlobStorage>(src_client, src_blob, read_settings, settings->max_single_read_retries,
            settings->max_single_download_retries);
        };

        UploadHelper helper{create_read_buffer, dest_client, offset, size, dest_container_for_logging, dest_blob, settings, schedule, for_disk_azure_blob_storage, &Poco::Logger::get("copyAzureBlobStorageFile")};
        helper.performCopy();
    }
}

}

#endif
