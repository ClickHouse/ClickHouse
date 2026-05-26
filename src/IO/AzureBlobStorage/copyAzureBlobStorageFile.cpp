#include <IO/AzureBlobStorage/copyAzureBlobStorageFile.h>

#if USE_AZURE_BLOB_STORAGE

#include <Common/ListWithMemoryTracking.h>
#include <Common/PODArray.h>
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

/// All ProfileEvent increments for Azure operations are now inside
/// `ContainerClientWrapper::traceAzure*` helpers; no externs needed here.


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
            BlobStorageLogWriterPtr blob_storage_log_,
            LoggerPtr log_)
            : create_read_buffer(create_read_buffer_)
            , client(client_)
            , offset (offset_)
            , total_size (total_size_)
            , dest_container_for_logging(dest_container_for_logging_)
            , dest_blob(dest_blob_)
            , settings(settings_)
            , schedule(schedule_)
            , blob_storage_log(std::move(blob_storage_log_))
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
        BlobStorageLogWriterPtr blob_storage_log;
        const LoggerPtr log;
        size_t max_single_part_upload_size;

        struct UploadPartTask
        {
            size_t part_offset{};
            size_t part_size{};
            Strings block_ids;
            bool is_finished = false;
        };

        size_t normal_part_size;
        Strings block_ids;

        ListWithMemoryTracking<UploadPartTask> TSA_GUARDED_BY(bg_tasks_mutex) bg_tasks;
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
            if (total_size < max_single_part_upload_size)
            {
                performSinglepartUpload();
            }
            else
            {
                performMultipartUpload();
            }
        }

        void performSinglepartUpload()
        {
            auto read_buffer = create_read_buffer();

            PODArray<char> memory;
            {
                memory.resize(total_size);
                WriteBufferFromVector<PODArray<char>> wb(memory);
                copyData(*read_buffer, wb, total_size);
            }

            client->uploadSinglePart(
                dest_blob, dest_container_for_logging,
                reinterpret_cast<const uint8_t *>(memory.data()),
                total_size,
                /* write_settings */ nullptr,   // copy path: no AccessConditions, no ResourceGuard
                /* num_tries     */ 1,          // copy path: no retry
                log, blob_storage_log);
        }

        void completeMultipartUpload()
        {
            client->commitBlockList(
                dest_blob, dest_container_for_logging, block_ids,
                /* write_settings */ nullptr,
                /* num_tries     */ 1,
                log, blob_storage_log);
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
            auto read_buffer = std::make_unique<LimitSeekableReadBuffer>(create_read_buffer(), task.part_offset, task.part_size);

            /// task.part_size is already normalized according to min_upload_part_size and max_upload_part_size.
            size_t size_to_stage = task.part_size;

            PODArray<char> memory;
            {
                memory.resize(size_to_stage);
                WriteBufferFromVector<PODArray<char>> wb(memory);
                copyData(*read_buffer, wb, size_to_stage);
            }

            const auto & block_id = task.block_ids.emplace_back(getRandomASCIIString(64));

            client->stageBlock(
                dest_blob, dest_container_for_logging, block_id,
                reinterpret_cast<const uint8_t *>(memory.data()),
                size_to_stage,
                /* write_settings */ nullptr,
                /* num_tries     */ 1,
                log, blob_storage_log);

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
    ThreadPoolCallbackRunnerUnsafe<void> schedule,
    BlobStorageLogWriterPtr blob_storage_log)
{
    auto log = getLogger("copyDataToAzureBlobStorageFile");
    UploadHelper helper{create_read_buffer, dest_client, offset, size, dest_container_for_logging, dest_blob, settings, schedule, std::move(blob_storage_log), log};
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
    const std::optional<ObjectAttributes> & object_to_attributes,
    ThreadPoolCallbackRunnerUnsafe<void> schedule,
    BlobStorageLogWriterPtr blob_storage_log)
{
    auto log = getLogger("copyAzureBlobStorageFile");
    bool is_native_copy_done = false;

    if (settings->use_native_copy && offset == 0) /// only native copy whole objects
    {
        /// Do native copy
        LOG_TRACE(log, "Copying Blob: {} from Container: {} using native copy", src_blob, src_container_for_logging);

        try
        {
            auto source_uri = src_client->GetBlobUrl(src_blob);

            if (size < settings->max_single_part_copy_size)
            {
                Azure::Storage::Blobs::CopyBlobFromUriOptions copy_options;
                if (object_to_attributes.has_value())
                {
                    for (const auto & [key, value] : *object_to_attributes)
                        copy_options.Metadata[key] = value;
                }

                LOG_TRACE(log, "Copy blob sync {} -> {} ...", source_uri, dest_blob);
                dest_client->copyBlobFromUri(dest_blob, source_uri, copy_options);
                LOG_TRACE(log, "Copy blob sync {} -> {} finished", source_uri, dest_blob);
            }
            else
            {
                Azure::Storage::Blobs::StartBlobCopyFromUriOptions copy_options;
                if (object_to_attributes.has_value())
                {
                    for (const auto & [key, value] : *object_to_attributes)
                        copy_options.Metadata[key] = value;
                }

                LOG_TRACE(log, "Copy blob sync {} -> {} ...", source_uri, dest_blob);
                auto properties_model = dest_client->copyBlobFromUriAsync(dest_blob, source_uri, copy_options);

                auto copy_status = properties_model.CopyStatus;
                auto copy_status_description = properties_model.CopyStatusDescription;

                if (copy_status.HasValue() && copy_status.Value() == Azure::Storage::Blobs::Models::CopyStatus::Success)
                {
                    LOG_TRACE(log, "Copy blob sync {} -> {} finished", source_uri, dest_blob);
                }
                else
                {
                    if (copy_status.HasValue())
                        throw Exception(ErrorCodes::AZURE_BLOB_STORAGE_ERROR, "Copy from {} to {} failed with status {} description {}",
                                        src_blob, dest_blob, copy_status.Value().ToString(), copy_status_description.Value());
                    throw Exception(
                        ErrorCodes::AZURE_BLOB_STORAGE_ERROR,
                        "Copy from {} to {} didn't complete with success status (copy source: {})",
                        src_blob,
                        dest_blob,
                        properties_model.CopySource.ValueOr("unknown"));
                }
            }
            is_native_copy_done = true;
        }
        catch (const Azure::Core::RequestFailedException & e)
        {
            if (e.StatusCode == Azure::Core::Http::HttpStatusCode::Unauthorized)
            {
                LOG_TRACE(log, "Copy operation has thrown unauthorized access error, which indicates that the storage account of the source & destination are not the same. "
                               "Will attempt to copy using read & write. source container = {} blob = {} and destination container = {} blob = {}",
                          src_container_for_logging, src_blob, dest_container_for_logging, dest_blob);
            }
            else if (e.StatusCode == Azure::Core::Http::HttpStatusCode::BadRequest)
            {
                LOG_TRACE(log, "Copy operation has thrown bad argument error. e.what = {}. "
                               "Will attempt to copy using read & write. source container = {} blob = {} and destination container = {} blob = {}",
                          e.what(), src_container_for_logging, src_blob, dest_container_for_logging, dest_blob);
            }
            else
                throw;
        }
    }
    if (!is_native_copy_done)
    {
        /// Copy through read and write
        LOG_TRACE(log, "Reading and writing Blob: {} from Container: {}", src_blob, src_container_for_logging);
        auto create_read_buffer = [&]
        {
            return std::make_unique<ReadBufferFromAzureBlobStorage>(
                src_client, src_blob, read_settings, settings->max_single_read_retries, settings->max_single_download_retries);
        };

        UploadHelper helper{create_read_buffer, dest_client, offset, size, dest_container_for_logging, dest_blob, settings, schedule, blob_storage_log, log};
        helper.performCopy();
    }
}

}

#endif
