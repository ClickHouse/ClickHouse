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
    extern const int LOGICAL_ERROR;
    extern const int INVALID_CONFIG_PARAMETER;
    extern const int AZURE_BLOB_STORAGE_ERROR;
}


size_t max_single_operation_copy_size = 256 * 1024 * 1024;


namespace
{
    class UploadHelper
    {
    public:
        UploadHelper(
            const CreateReadBuffer & create_read_buffer_,
            MultiVersion<Azure::Storage::Blobs::BlobContainerClient> & client_,
            size_t offset_,
            size_t total_size_,
            const String & dest_container_,
            const String & dest_blob_,
            std::shared_ptr<AzureObjectStorageSettings> settings_,
            const std::optional<std::map<String, String>> & object_metadata_,
            ThreadPoolCallbackRunner<void> schedule_,
            bool for_disk_azure_blob_storage_,
            const Poco::Logger * log_)
            : create_read_buffer(create_read_buffer_)
            , client(client_)
            , offset (offset_)
            , total_size (total_size_)
            , dest_container(dest_container_)
            , dest_blob(dest_blob_)
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
        MultiVersion<Azure::Storage::Blobs::BlobContainerClient> & client;
        size_t offset;
        size_t total_size;
        const String & dest_container;
        const String & dest_blob;
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

        void calculatePartSize()
        {
            if (!total_size)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Chosen multipart upload for an empty file. This must not happen");

            auto max_part_number = settings.get()->max_part_number;
            auto min_upload_part_size = settings.get()->min_upload_part_size;
            auto max_upload_part_size = settings.get()->max_upload_part_size;

            if (!max_part_number)
                throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "max_part_number must not be 0");
            else if (!min_upload_part_size)
                throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "min_upload_part_size must not be 0");
            else if (max_upload_part_size < min_upload_part_size)
                throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "max_upload_part_size must not be less than min_upload_part_size");

            size_t part_size = min_upload_part_size;
            size_t num_parts = (total_size + part_size - 1) / part_size;

            if (num_parts > max_part_number)
            {
                part_size = (total_size + max_part_number - 1) / max_part_number;
                num_parts = (total_size + part_size - 1) / part_size;
            }

            if (part_size > max_upload_part_size)
            {
                part_size = max_upload_part_size;
                num_parts = (total_size + part_size - 1) / part_size;
            }

            if (num_parts < 1 || num_parts > max_part_number || part_size < min_upload_part_size || part_size > max_upload_part_size)
            {
                String msg;
                if (num_parts < 1)
                    msg = "Number of parts is zero";
                else if (num_parts > max_part_number)
                    msg = fmt::format("Number of parts exceeds {}", num_parts, max_part_number);
                else if (part_size < min_upload_part_size)
                    msg = fmt::format("Size of a part is less than {}", part_size, min_upload_part_size);
                else
                    msg = fmt::format("Size of a part exceeds {}", part_size, max_upload_part_size);

                throw Exception(
                    ErrorCodes::INVALID_CONFIG_PARAMETER,
                    "{} while writing {} bytes to AzureBlobStorage. Check max_part_number = {}, "
                    "min_upload_part_size = {}, max_upload_part_size = {}",
                    msg, total_size, max_part_number, min_upload_part_size, max_upload_part_size);
            }

            /// We've calculated the size of a normal part (the final part can be smaller).
            normal_part_size = part_size;
        }

    public:
        void performCopy()
        {
            performMultipartUpload();
        }

        void completeMultipartUpload()
        {
            auto block_blob_client = client.get()->GetBlockBlobClient(dest_blob);
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
            LOG_TRACE(log, "Writing part. Container: {}, Blob: {}, Size: {}", dest_container, dest_blob, part_size);

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
            LOG_TRACE(log, "Writing part finished. Container: {}, Blob: {}, block_id: {}, Parts: {}", dest_container, dest_blob, block_id, bg_tasks.size());
        }

        String processUploadPartRequest(UploadPartTask & task)
        {
            ProfileEvents::increment(ProfileEvents::AzureUploadPart);
            if (for_disk_azure_blob_storage)
                ProfileEvents::increment(ProfileEvents::DiskAzureUploadPart);

            auto block_blob_client = client.get()->GetBlockBlobClient(dest_blob);
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
    MultiVersion<Azure::Storage::Blobs::BlobContainerClient> & dest_client,
    const String & dest_container,
    const String & dest_blob,
    std::shared_ptr<AzureObjectStorageSettings> settings,
    const std::optional<std::map<String, String>> & object_metadata,
    ThreadPoolCallbackRunner<void> schedule,
    bool for_disk_azure_blob_storage)
{
    UploadHelper helper{create_read_buffer, dest_client, offset, size, dest_container, dest_blob, settings, object_metadata, schedule, for_disk_azure_blob_storage, &Poco::Logger::get("copyDataToAzureBlobStorageFile")};
    helper.performCopy();
}


void copyAzureBlobStorageFile(
    MultiVersion<Azure::Storage::Blobs::BlobContainerClient> & src_client,
    MultiVersion<Azure::Storage::Blobs::BlobContainerClient> & dest_client,
    const String & src_container,
    const String & src_blob,
    size_t offset,
    size_t size,
    const String & dest_container,
    const String & dest_blob,
    std::shared_ptr<AzureObjectStorageSettings> settings,
    const ReadSettings & read_settings,
    const std::optional<std::map<String, String>> & object_metadata,
    ThreadPoolCallbackRunner<void> schedule,
    bool for_disk_azure_blob_storage)
{

    if (settings->use_native_copy)
    {
        ProfileEvents::increment(ProfileEvents::AzureCopyObject);
        if (for_disk_azure_blob_storage)
            ProfileEvents::increment(ProfileEvents::DiskAzureCopyObject);

        auto block_blob_client_src = src_client.get()->GetBlockBlobClient(src_blob);
        auto block_blob_client_dest = dest_client.get()->GetBlockBlobClient(dest_blob);
        auto source_uri = block_blob_client_src.GetUrl();

        if (size < max_single_operation_copy_size)
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
        LOG_TRACE(&Poco::Logger::get("copyAzureBlobStorageFile"), "Reading from Container: {}, Blob: {}", src_container, src_blob);
        auto create_read_buffer = [&]
        {
            return std::make_unique<ReadBufferFromAzureBlobStorage>(src_client.get(), src_blob, read_settings, settings->max_single_read_retries,
            settings->max_single_download_retries);
        };

        UploadHelper helper{create_read_buffer, dest_client, offset, size, dest_container, dest_blob, settings, object_metadata, schedule, for_disk_azure_blob_storage, &Poco::Logger::get("copyAzureBlobStorageFile")};
        helper.performCopy();
    }
}

}

#endif
