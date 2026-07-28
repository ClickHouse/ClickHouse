#include <IO/AzureBlobStorage/copyAzureBlobStorageFile.h>

#if USE_AZURE_BLOB_STORAGE

#include <Common/PODArray.h>
#include <Common/ThreadPoolTaskTracker.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
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

        size_t normal_part_size;
        /// One block id per part, indexed by part number so that `completeMultipartUpload`
        /// commits the blocks in the right order regardless of the order parts finish in.
        Strings block_ids;

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
            auto block_blob_client = client->GetBlockBlobClient(dest_blob);
            auto read_buffer = create_read_buffer();

            PODArray<char> memory;
            {
                memory.resize(total_size);
                WriteBufferFromVector<PODArray<char>> wb(memory);
                copyData(*read_buffer, wb, total_size);
            }

            Azure::Core::IO::MemoryBodyStream stream(reinterpret_cast<const uint8_t *>(memory.data()), total_size);

            Stopwatch watch;
            Int32 error_code = 0;
            String error_message;
            try
            {
                block_blob_client.Upload(stream);
            }
            catch (const Azure::Core::RequestFailedException & e)
            {
                error_code = static_cast<Int32>(e.StatusCode);
                error_message = e.Message;
                if (blob_storage_log)
                    blob_storage_log->addEvent(
                        BlobStorageLogElement::EventType::Upload,
                        /* bucket */ dest_container_for_logging,
                        /* remote_path */ dest_blob,
                        /* local_path */ {},
                        /* data_size */ total_size,
                        watch.elapsedMicroseconds(),
                        error_code,
                        error_message);
                throw;
            }
        }

        void completeMultipartUpload()
        {
            auto block_blob_client = client->GetBlockBlobClient(dest_blob);
            ProfileEvents::increment(ProfileEvents::AzureCommitBlockList);
            if (client->IsClientForDisk())
                ProfileEvents::increment(ProfileEvents::DiskAzureCommitBlockList);

            Stopwatch watch;
            Int32 error_code = 0;
            String error_message;
            try
            {
                block_blob_client.CommitBlockList(block_ids);
            }
            catch (const Azure::Core::RequestFailedException & e)
            {
                error_code = static_cast<Int32>(e.StatusCode);
                error_message = e.Message;
                if (blob_storage_log)
                    blob_storage_log->addEvent(
                        BlobStorageLogElement::EventType::MultiPartUploadComplete,
                        /* bucket */ dest_container_for_logging,
                        /* remote_path */ dest_blob,
                        /* local_path */ {},
                        /* data_size */ 0,
                        watch.elapsedMicroseconds(),
                        error_code,
                        error_message);
                throw;
            }
            auto elapsed = watch.elapsedMicroseconds();

            if (blob_storage_log)
                blob_storage_log->addEvent(
                    BlobStorageLogElement::EventType::MultiPartUploadComplete,
                    /* bucket */ dest_container_for_logging,
                    /* remote_path */ dest_blob,
                    /* local_path */ {},
                    /* data_size */ 0,
                    elapsed,
                    error_code,
                    error_message);
        }

        void performMultipartUpload()
        {
            calculatePartSize();

            size_t num_parts = (total_size + normal_part_size - 1) / normal_part_size;
            block_ids.resize(num_parts);

            size_t position = offset;
            size_t end_position = offset + total_size;

            LogSeriesLimiterPtr limited_log = std::make_shared<LogSeriesLimiter>(log, 1, 5);
            /// Bound the number of parts staged concurrently. Every in-flight part holds a full
            /// `part_size` buffer in memory, so without this limit a large file (or many files
            /// copied in parallel on the backups IO thread pool) could schedule all parts at once
            /// and blow up memory usage.
            TaskTracker task_tracker(schedule, settings->max_inflight_parts_for_one_file, limited_log);

            try
            {
                for (size_t part_index = 0; position < end_position; ++part_index)
                {
                    size_t next_position = std::min(position + normal_part_size, end_position);
                    size_t part_size = next_position - position; /// `part_size` is either `normal_part_size` or smaller if it's the final part.

                    task_tracker.add([this, part_index, position, part_size]()
                    {
                        processUploadPartRequest(part_index, position, part_size);
                    });

                    position = next_position;
                }

                task_tracker.waitAll();
                completeMultipartUpload();
            }
            catch (...)
            {
                tryLogCurrentException(log, fmt::format("While performing multipart upload of blob {} in container {}", dest_blob, dest_container_for_logging));
                task_tracker.safeWaitAll();
                throw;
            }
        }

        void processUploadPartRequest(size_t part_index, size_t part_offset, size_t part_size)
        {
            LOG_TRACE(log, "Writing part. Container: {}, Blob: {}, Size: {}", dest_container_for_logging, dest_blob, part_size);

            ProfileEvents::increment(ProfileEvents::AzureStageBlock);
            if (client->IsClientForDisk())
                ProfileEvents::increment(ProfileEvents::DiskAzureStageBlock);

            auto block_blob_client = client->GetBlockBlobClient(dest_blob);
            auto read_buffer = std::make_unique<LimitSeekableReadBuffer>(create_read_buffer(), part_offset, part_size);

            /// part_size is already normalized according to min_upload_part_size and max_upload_part_size.
            size_t size_to_stage = part_size;

            PODArray<char> memory;
            {
                memory.resize(size_to_stage);
                WriteBufferFromVector<PODArray<char>> wb(memory);
                copyData(*read_buffer, wb, size_to_stage);
            }

            Azure::Core::IO::MemoryBodyStream stream(reinterpret_cast<const uint8_t *>(memory.data()), size_to_stage);

            auto block_id = getRandomASCIIString(64);
            block_ids[part_index] = block_id;

            Stopwatch watch;
            Int32 error_code = 0;
            String error_message;
            try
            {
                block_blob_client.StageBlock(block_id, stream);
            }
            catch (const Azure::Core::RequestFailedException & e)
            {
                error_code = static_cast<Int32>(e.StatusCode);
                error_message = e.Message;
                if (blob_storage_log)
                    blob_storage_log->addEvent(
                        BlobStorageLogElement::EventType::MultiPartUploadWrite,
                        /* bucket */ dest_container_for_logging,
                        /* remote_path */ dest_blob,
                        /* local_path */ {},
                        /* data_size */ size_to_stage,
                        watch.elapsedMicroseconds(),
                        error_code,
                        error_message);
                throw;
            }
            auto elapsed = watch.elapsedMicroseconds();

            if (blob_storage_log)
                blob_storage_log->addEvent(
                    BlobStorageLogElement::EventType::MultiPartUploadWrite,
                    /* bucket */ dest_container_for_logging,
                    /* remote_path */ dest_blob,
                    /* local_path */ {},
                    /* data_size */ size_to_stage,
                    elapsed,
                    error_code,
                    error_message);

            LOG_TRACE(log, "Writing part. Container: {}, Blob: {}, block_id: {}, size: {}",
                      dest_container_for_logging, dest_blob, block_id, size_to_stage);
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

    if (settings->use_native_copy)
    {
        /// Do native copy
        LOG_TRACE(log, "Copying Blob: {} from Container: {} using native copy", src_blob, src_container_for_logging);
        ProfileEvents::increment(ProfileEvents::AzureCopyObject);
        if (dest_client->IsClientForDisk())
            ProfileEvents::increment(ProfileEvents::DiskAzureCopyObject);

        try
        {
            auto block_blob_client_src = src_client->GetBlockBlobClient(src_blob);
            auto block_blob_client_dest = dest_client->GetBlockBlobClient(dest_blob);

            auto source_uri = block_blob_client_src.GetUrl();

            if (size < settings->max_single_part_copy_size)
            {
                Azure::Storage::Blobs::CopyBlobFromUriOptions copy_options;
                if (object_to_attributes.has_value())
                {
                    for (const auto & [key, value] : *object_to_attributes)
                        copy_options.Metadata[key] = value;
                }

                LOG_TRACE(log, "Copy blob sync {} -> {}", src_blob, dest_blob);
                block_blob_client_dest.CopyFromUri(source_uri, copy_options);
            }
            else
            {
                Azure::Storage::Blobs::StartBlobCopyFromUriOptions copy_options;
                if (object_to_attributes.has_value())
                {
                    for (const auto & [key, value] : *object_to_attributes)
                        copy_options.Metadata[key] = value;
                }

                Azure::Storage::Blobs::StartBlobCopyOperation operation = block_blob_client_dest.StartCopyFromUri(source_uri, copy_options);

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
            is_native_copy_done = true;
        }
        catch (const Azure::Storage::StorageException & e)
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
