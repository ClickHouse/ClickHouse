#include <IO/S3/deleteFileFromS3.h>

#if USE_AWS_S3

#include <Common/logger_useful.h>
#include <IO/S3/Client.h>
#include <IO/S3/Requests.h>
#include <IO/S3/BlobStorageLogWriter.h>
#include <IO/S3/S3Capabilities.h>
#include <IO/S3/getObjectInfo.h>


namespace ProfileEvents
{
    extern const Event S3DeleteObjects;
}


namespace DB
{

void deleteFileFromS3(
    const std::shared_ptr<const S3::Client> & s3_client,
    const String & bucket,
    const String & key,
    bool if_exists,
    BlobStorageLogWriterPtr blob_storage_log,
    const String & local_path_for_blob_storage_log,
    size_t file_size_for_blob_storage_log,
    std::optional<ProfileEvents::Event> profile_event)
{
    ProfileEvents::increment(ProfileEvents::S3DeleteObjects);
    if (profile_event && *profile_event != ProfileEvents::S3DeleteObjects)
        ProfileEvents::increment(*profile_event);

    auto log = getLogger("deleteFileFromS3");

    S3::DeleteObjectRequest request;
    request.SetBucket(bucket);
    request.SetKey(key);
    auto outcome = s3_client->DeleteObject(request);

    if (blob_storage_log)
    {
        blob_storage_log->addEvent(BlobStorageLogElement::EventType::Delete,
                                   bucket, key,
                                   local_path_for_blob_storage_log, file_size_for_blob_storage_log,
                                   outcome.IsSuccess() ? nullptr : &outcome.GetError());
    }
    else
    {
        LOG_TRACE(log, "No blob storage log, not writing blob {}", key);
    }

    if (!outcome.IsSuccess() && (!if_exists || !S3::isNotFoundError(outcome.GetError().GetErrorType())))
    {
        /// In this case even if absence of key may be ok for us, the log will be polluted with error messages from aws sdk.
        /// Looks like there is no way to suppress them.
        const auto & err = outcome.GetError();
        throw S3Exception(err.GetErrorType(), "{} (Code: {}) while removing object with path {} from S3",
                          err.GetMessage(), static_cast<size_t>(err.GetErrorType()), key);
    }

    LOG_DEBUG(log, "Object with path {} was removed from S3", key);
}


void deleteFilesFromS3(
    const std::shared_ptr<const S3::Client> & s3_client,
    const String & bucket,
    const Strings & keys,
    bool if_exists,
    std::optional<bool> is_batch_delete_supported,
    std::function<void(bool)> set_is_batch_delete_supported,
    size_t batch_size,
    BlobStorageLogWriterPtr blob_storage_log,
    const Strings & local_paths_for_blob_storage_log,
    const std::vector<size_t> & file_sizes_for_blob_storage_log,
    std::optional<ProfileEvents::Event> profile_event)
{
    if (keys.empty())
       return; /// Nothing to delete.

    String empty_string;

    if (keys.size() == 1)
    {
        /// We're deleting one file - there is no need for the batch delete.
        const String & local_path_for_blob_storage_log = !local_paths_for_blob_storage_log.empty() ? local_paths_for_blob_storage_log[0] : empty_string;
        size_t file_size_for_blob_storage_log = !file_sizes_for_blob_storage_log.empty() ? file_sizes_for_blob_storage_log[0] : 0;
        deleteFileFromS3(s3_client, bucket, keys[0], if_exists,
                         blob_storage_log, local_path_for_blob_storage_log, file_size_for_blob_storage_log,
                         profile_event);
        return;
    }

    auto log = getLogger("deleteFileFromS3");

    /// We're trying the batch delete first.
    bool try_batch_requests = true;
    {
        if (is_batch_delete_supported.has_value() && !is_batch_delete_supported.value())
            try_batch_requests = false;
        else if (batch_size < 2)
            try_batch_requests = false;
    }

    if (try_batch_requests)
    {
        bool need_retry_with_plain_delete_object = false;
        size_t current_position = 0;

        while (current_position < keys.size())
        {
            std::vector<Aws::S3::Model::ObjectIdentifier> current_chunk;
            String comma_separated_keys;
            size_t first_position = current_position;
            for (; current_position < keys.size() && current_chunk.size() < batch_size; ++current_position)
            {
                Aws::S3::Model::ObjectIdentifier obj;
                obj.SetKey(keys[current_position]);
                current_chunk.push_back(obj);

                if (!comma_separated_keys.empty())
                    comma_separated_keys += ", ";
                comma_separated_keys += keys[current_position];
            }

            Aws::S3::Model::Delete delkeys;
            delkeys.SetObjects(current_chunk);

            ProfileEvents::increment(ProfileEvents::S3DeleteObjects);
            if (profile_event && *profile_event != ProfileEvents::S3DeleteObjects)
                ProfileEvents::increment(*profile_event);

            S3::DeleteObjectsRequest request;
            request.SetBucket(bucket);
            request.SetDelete(delkeys);
            auto outcome = s3_client->DeleteObjects(request);

            if (blob_storage_log)
            {
                const auto * outcome_error = outcome.IsSuccess() ? nullptr : &outcome.GetError();
                auto time_now = std::chrono::system_clock::now();
                LOG_TRACE(log, "Writing Delete operation for blobs [{}]", comma_separated_keys);
                for (size_t i = first_position; i < current_position; ++i)
                {
                    const String & local_path_for_blob_storage_log = (i < local_paths_for_blob_storage_log.size()) ? local_paths_for_blob_storage_log[i] : empty_string;
                    size_t file_size_for_blob_storage_log = (i < file_sizes_for_blob_storage_log.size()) ? file_sizes_for_blob_storage_log[i] : 0;

                    blob_storage_log->addEvent(BlobStorageLogElement::EventType::Delete,
                                               bucket, keys[i],
                                               local_path_for_blob_storage_log, file_size_for_blob_storage_log,
                                               outcome_error, time_now);
                }
            }
            else
            {
                LOG_TRACE(log, "No blob storage log, not writing blobs [{}]", comma_separated_keys);
            }

            if (!is_batch_delete_supported.has_value())
            {
                if (!outcome.IsSuccess()
                    && (outcome.GetError().GetExceptionName() == "InvalidRequest"
                        || outcome.GetError().GetExceptionName() == "InvalidArgument"))
                {
                    const auto & err = outcome.GetError();
                    LOG_TRACE(log, "DeleteObjects is not supported: {} (Code: {}). Retrying with plain DeleteObject.",
                              err.GetMessage(), static_cast<size_t>(err.GetErrorType()));
                    is_batch_delete_supported = false;
                    if (set_is_batch_delete_supported)
                        set_is_batch_delete_supported(*is_batch_delete_supported);
                    need_retry_with_plain_delete_object = true;
                    break;
                }
            }

            if (!outcome.IsSuccess() && (!if_exists || !S3::isNotFoundError(outcome.GetError().GetErrorType())))
            {
                /// In this case even if absence of key may be ok for us, the log will be polluted with error messages from aws sdk.
                /// Looks like there is no way to suppress them.
                const auto & err = outcome.GetError();
                throw S3Exception(err.GetErrorType(), "{} (Code: {}) while removing objects with paths [{}] from S3",
                                  err.GetMessage(), static_cast<size_t>(err.GetErrorType()), comma_separated_keys);
            }

            is_batch_delete_supported = true;
            if (set_is_batch_delete_supported)
                set_is_batch_delete_supported(*is_batch_delete_supported);

            LOG_DEBUG(log, "Objects with paths [{}] were removed from S3", comma_separated_keys);
        }

        if (!need_retry_with_plain_delete_object)
            return;
    }

    /// The batch delete isn't supported so we'll delete all files sequentially.
    for (size_t i = 0; i != keys.size(); ++i)
    {
        const String & local_path_for_blob_storage_log = (i < local_paths_for_blob_storage_log.size()) ? local_paths_for_blob_storage_log[i] : empty_string;
        size_t file_size_for_blob_storage_log = (i < file_sizes_for_blob_storage_log.size()) ? file_sizes_for_blob_storage_log[i] : 0;

        deleteFileFromS3(s3_client, bucket, keys[i], if_exists,
                         blob_storage_log, local_path_for_blob_storage_log, file_size_for_blob_storage_log,
                         profile_event);
    }
}


void deleteFilesFromS3(
    const std::shared_ptr<const S3::Client> & s3_client,
    const String & bucket,
    const Strings & keys,
    bool if_exists,
    S3Capabilities & s3_capabilities,
    size_t batch_size,
    BlobStorageLogWriterPtr blob_storage_log,
    const Strings & local_paths_for_blob_storage_log,
    const std::vector<size_t> & file_sizes_for_blob_storage_log,
    std::optional<ProfileEvents::Event> profile_event)
{
    std::optional<bool> is_batch_delete_supported = s3_capabilities.is_batch_delete_supported();

    auto set_is_batch_delete_supported
        = [&](bool support_batch_delete_) { s3_capabilities.set_is_batch_delete_supported(support_batch_delete_); };

    deleteFilesFromS3(s3_client, bucket, keys, if_exists, is_batch_delete_supported, set_is_batch_delete_supported, batch_size,
                      blob_storage_log, local_paths_for_blob_storage_log, file_sizes_for_blob_storage_log, profile_event);
}

}

#endif
