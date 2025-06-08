#include <IO/AzureBlobStorage/copyAzureBlobStorageFile.h>

#if USE_AZURE_BLOB_STORAGE

#include <Common/PODArray.h>
#include <Common/ProfileEvents.h>
#include <Common/typeid_cast.h>
#include <Interpreters/Context.h>
#include <IO/LimitSeekableReadBuffer.h>
#include <IO/SeekableReadBuffer.h>
#include <IO/SharedThreadPools.h>
#include <IO/WriteBufferFromVector.h>
#include <Disks/IO/ReadBufferFromAzureBlobStorage.h>
#include <Disks/IO/WriteBufferFromAzureBlobStorage.h>
#include <Common/getRandomASCIIString.h>


#include <azure/core/credentials/credentials.hpp>
#include <azure/storage/common/storage_credential.hpp>
#include <azure/identity/managed_identity_credential.hpp>
#include <azure/identity/workload_identity_credential.hpp>

namespace ProfileEvents
{
    extern const Event AzureCopyObject;
    extern const Event AzureStageBlock;
    extern const Event AzureCommitBlockList;

    extern const Event DiskAzureCopyObject;
    extern const Event DiskAzureStageBlock;
    extern const Event DiskAzureCommitBlockList;
}


namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_CONFIG_PARAMETER;
    extern const int AZURE_BLOB_STORAGE_ERROR;
    extern const int LOGICAL_ERROR;
}

namespace
{
    class UploadHelper
    {
    public:
        UploadHelper(
            const CreateReadBuffer & create_read_buffer_,
            std::shared_ptr<const AzureBlobStorage::ContainerClient> client_,
            size_t offset_,
            size_t total_size_,
            const String & dest_container_for_logging_,
            const String & dest_blob_,
            std::shared_ptr<const AzureBlobStorage::RequestSettings> settings_,
            ThreadPoolCallbackRunnerUnsafe<void> schedule_,
            LoggerPtr log_)
            : create_read_buffer(create_read_buffer_)
            , client(client_)
            , offset (offset_)
            , total_size (total_size_)
            , dest_container_for_logging(dest_container_for_logging_)
            , dest_blob(dest_blob_)
            , settings(settings_)
            , schedule(schedule_)
            , log(log_)
            , max_single_part_upload_size(settings_->max_single_part_upload_size)
            , normal_part_size(0)
        {
        }

        virtual ~UploadHelper() = default;

    protected:
        std::function<std::unique_ptr<SeekableReadBuffer>()> create_read_buffer;
        std::shared_ptr<const AzureBlobStorage::ContainerClient> client;
        size_t offset;
        size_t total_size;
        const String & dest_container_for_logging;
        const String & dest_blob;
        std::shared_ptr<const AzureBlobStorage::RequestSettings> settings;
        ThreadPoolCallbackRunnerUnsafe<void> schedule;
        const LoggerPtr log;
        size_t max_single_part_upload_size;

        struct UploadPartTask
        {
            size_t part_offset;
            size_t part_size;
            std::vector<std::string> block_ids;
            bool is_finished = false;
        };

        size_t normal_part_size;
        std::vector<std::string> block_ids;

        std::list<UploadPartTask> TSA_GUARDED_BY(bg_tasks_mutex) bg_tasks;
        int num_added_bg_tasks TSA_GUARDED_BY(bg_tasks_mutex) = 0;
        int num_finished_bg_tasks TSA_GUARDED_BY(bg_tasks_mutex) = 0;
        std::exception_ptr bg_exception TSA_GUARDED_BY(bg_tasks_mutex);
        std::mutex bg_tasks_mutex;
        std::condition_variable bg_tasks_condvar;

        void calculatePartSize()
        {
            if (!total_size)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Chosen multipart upload for an empty file. This must not happen");

            auto max_part_number = settings->max_blocks_in_multipart_upload;
            const auto min_upload_part_size = settings->min_upload_part_size;
            const auto max_upload_part_size = settings->max_upload_part_size;

            if (!max_part_number)
                throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "max_blocks_in_multipart_upload must not be 0");
            if (!min_upload_part_size)
                throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "min_upload_part_size must not be 0");
            if (max_upload_part_size < min_upload_part_size)
                throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "max_upload_part_size must not be less than min_upload_part_size");

            size_t part_size = min_upload_part_size;
            auto num_parts = (total_size + part_size - 1) / part_size;

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

            String error;
            if (num_parts < 1)
                error = "Number of parts is zero";
            else if (num_parts > max_part_number)
                error = fmt::format("Number of parts exceeds {}/{}", num_parts, max_part_number);
            else if (part_size < min_upload_part_size)
                error = fmt::format("Size of a part is less than {}/{}", part_size, min_upload_part_size);
            else if (part_size > max_upload_part_size)
                error = fmt::format("Size of a part exceeds {}/{}", part_size, max_upload_part_size);

            if (!error.empty())
            {
                throw Exception(
                    ErrorCodes::INVALID_CONFIG_PARAMETER,
                    "{} while writing {} bytes to Azure. Check max_part_number = {}, "
                    "min_upload_part_size = {}, max_upload_part_size = {}",
                    error, total_size, max_part_number, min_upload_part_size, max_upload_part_size);
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
            auto block_blob_client = client->GetBlockBlobClient(dest_blob);
            ProfileEvents::increment(ProfileEvents::AzureCommitBlockList);
            if (client->IsClientForDisk())
                ProfileEvents::increment(ProfileEvents::DiskAzureCommitBlockList);

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
                tryLogCurrentException(log, fmt::format("While performing multipart upload of blob {} in container {}", dest_blob, dest_container_for_logging));
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
                            std::lock_guard lock(bg_tasks_mutex);
                            if (!bg_exception)
                            {
                                tryLogCurrentException(log, "While writing part");
                                bg_exception = std::current_exception(); /// The exception will be rethrown after all background tasks stop working.
                            }
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
            ProfileEvents::increment(ProfileEvents::AzureStageBlock);
            if (client->IsClientForDisk())
                ProfileEvents::increment(ProfileEvents::DiskAzureStageBlock);

            auto block_blob_client = client->GetBlockBlobClient(dest_blob);
            auto read_buffer = std::make_unique<LimitSeekableReadBuffer>(create_read_buffer(), task.part_offset, task.part_size);

            /// task.part_size is already normalized according to min_upload_part_size and max_upload_part_size.
            size_t size_to_stage = task.part_size;

            PODArray<char> memory;
            {
                memory.resize(size_to_stage);
                WriteBufferFromVector<PODArray<char>> wb(memory);
                copyData(*read_buffer, wb, size_to_stage);
            }

            Azure::Core::IO::MemoryBodyStream stream(reinterpret_cast<const uint8_t *>(memory.data()), size_to_stage);

            const auto & block_id = task.block_ids.emplace_back(getRandomASCIIString(64));
            block_blob_client.StageBlock(block_id, stream);

            LOG_TRACE(log, "Writing part. Container: {}, Blob: {}, block_id: {}, size: {}",
                      dest_container_for_logging, dest_blob, block_id, size_to_stage);
        }


        void waitForAllBackgroundTasks()
        {
            if (!schedule)
                return;

            std::unique_lock lock(bg_tasks_mutex);
            /// Suppress warnings because bg_tasks_mutex is actually hold, but tsa annotations do not understand std::unique_lock
            bg_tasks_condvar.wait(lock, [this]() {return TSA_SUPPRESS_WARNING_FOR_READ(num_added_bg_tasks) == TSA_SUPPRESS_WARNING_FOR_READ(num_finished_bg_tasks); });

            auto exception = TSA_SUPPRESS_WARNING_FOR_READ(bg_exception);
            if (exception)
                std::rethrow_exception(exception);

            const auto & tasks = TSA_SUPPRESS_WARNING_FOR_READ(bg_tasks);
            for (const auto & task : tasks)
                block_ids.insert(block_ids.end(),task.block_ids.begin(), task.block_ids.end());
        }
    };
}


void copyDataToAzureBlobStorageFile(
    const std::function<std::unique_ptr<SeekableReadBuffer>()> & create_read_buffer,
    size_t offset,
    size_t size,
    std::shared_ptr<const AzureBlobStorage::ContainerClient> dest_client,
    const String & dest_container_for_logging,
    const String & dest_blob,
    std::shared_ptr<const AzureBlobStorage::RequestSettings> settings,
    ThreadPoolCallbackRunnerUnsafe<void> schedule)
{
    auto log = getLogger("copyDataToAzureBlobStorageFile");
    UploadHelper helper{create_read_buffer, dest_client, offset, size, dest_container_for_logging, dest_blob, settings, schedule, log};
    helper.performCopy();
}


void copyAzureBlobStorageFile(
    std::shared_ptr<const AzureBlobStorage::ContainerClient> src_client,
    std::shared_ptr<const AzureBlobStorage::ContainerClient> dest_client,
    const String & src_container_for_logging,
    const String & src_blob,
    size_t offset,
    size_t size,
    const String & dest_container_for_logging,
    const String & dest_blob,
    std::shared_ptr<const AzureBlobStorage::RequestSettings> settings,
    const ReadSettings & read_settings,
    bool same_credentials,
    ThreadPoolCallbackRunnerUnsafe<void> schedule)
{
    auto log = getLogger("copyAzureBlobStorageFile");

    if (settings->use_native_copy && same_credentials)
    {
        LOG_TRACE(log, "Copying Blob: {} from Container: {} using native copy", src_blob, src_container_for_logging);
        ProfileEvents::increment(ProfileEvents::AzureCopyObject);
        if (dest_client->IsClientForDisk())
            ProfileEvents::increment(ProfileEvents::DiskAzureCopyObject);

        auto block_blob_client_src = src_client->GetBlockBlobClient(src_blob);
        auto block_blob_client_dest = dest_client->GetBlockBlobClient(dest_blob);

        auto source_uri = block_blob_client_src.GetUrl();

        if (size < settings->max_single_part_copy_size)
        {
            LOG_TRACE(log, "Copy blob sync {} -> {}", src_blob, dest_blob);
            block_blob_client_dest.CopyFromUri(source_uri);
        }
        else
        {
            Azure::Storage::Blobs::StartBlobCopyOperation operation = block_blob_client_dest.StartCopyFromUri(source_uri);

            auto copy_response = operation.PollUntilDone(std::chrono::milliseconds(100));
            auto properties_model = copy_response.Value;

            auto copy_status = properties_model.CopyStatus;
            auto copy_status_description = properties_model.CopyStatusDescription;


            if (copy_status.HasValue() && copy_status.Value() == Azure::Storage::Blobs::Models::CopyStatus::Success)
            {
                LOG_TRACE(log, "Copy of {} to {} finished", properties_model.CopySource.Value(), dest_blob);
            }
            else
            {
                if (copy_status.HasValue())
                    throw Exception(ErrorCodes::AZURE_BLOB_STORAGE_ERROR, "Copy from {} to {} failed with status {} description {} (operation is done {})",
                                    src_blob, dest_blob, copy_status.Value().ToString(), copy_status_description.Value(), operation.IsDone());
                throw Exception(
                    ErrorCodes::AZURE_BLOB_STORAGE_ERROR,
                    "Copy from {} to {} didn't complete with success status (operation is done {})",
                    src_blob,
                    dest_blob,
                    operation.IsDone());
            }
        }
    }
    else
    {
        LOG_TRACE(log, "Copying Blob: {} from Container: {} native copy is disabled {}", src_blob, src_container_for_logging, same_credentials ? "" : " because of different credentials");
        auto create_read_buffer = [&]
        {
            return std::make_unique<ReadBufferFromAzureBlobStorage>(
                src_client, src_blob, read_settings, settings->max_single_read_retries, settings->max_single_download_retries);
        };

        UploadHelper helper{create_read_buffer, dest_client, offset, size, dest_container_for_logging, dest_blob, settings, schedule, log};
        helper.performCopy();
    }
}

}

#endif
