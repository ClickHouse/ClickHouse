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
    S3::DeleteObjectRequest request;
    request.SetBucket(bucket);
    request.SetKey(key);

    ProfileEvents::increment(ProfileEvents::S3DeleteObjects);
    if (profile_event && *profile_event != ProfileEvents::S3DeleteObjects)
        ProfileEvents::increment(*profile_event);

    auto outcome = s3_client->DeleteObject(request);

    auto log = getLogger("deleteFileFromS3");

    if (blob_storage_log)
    {
        LOG_TRACE(log, "Writing Delete operation for blob {}", key);
        blob_storage_log->addEvent(BlobStorageLogElement::EventType::Delete,
                                   bucket, key,
                                   local_path_for_blob_storage_log, file_size_for_blob_storage_log,
                                   outcome.IsSuccess() ? nullptr : &outcome.GetError());
    }
    else
    {
        LOG_TRACE(log, "No blob storage log, not writing blob {}", key);
    }

    if (outcome.IsSuccess())
    {
        LOG_DEBUG(log, "Object with path {} was removed from S3", key);
    }
    else if (if_exists && S3::isNotFoundError(outcome.GetError().GetErrorType()))
    {
        /// In this case even if absence of key may be ok for us, the log will be polluted with error messages from aws sdk.
        /// Looks like there is no way to suppress them.
        LOG_TRACE(log, "Object with path {} was skipped because it didn't exist", key);
    }
    else
    {
        const auto & err = outcome.GetError();
        throw S3Exception(err.GetErrorType(), "{} (Code: {}) while removing object with path {} from S3",
                          err.GetMessage(), static_cast<size_t>(err.GetErrorType()), key);
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
    chassert(local_paths_for_blob_storage_log.empty() || (local_paths_for_blob_storage_log.size() == keys.size()));
    chassert(file_sizes_for_blob_storage_log.empty() || (file_sizes_for_blob_storage_log.size() == keys.size()));

    if (keys.empty())
       return; /// Nothing to delete.

    /// We're trying batch delete (DeleteObjects) first.
    bool try_batch_delete = true;
    {
        if (keys.size() == 1)
            try_batch_delete = false; /// We're deleting one file - there is no need for batch delete.
        else if (batch_size < 2)
            try_batch_delete = false; /// Can't do batch delete with such small batches.
        else if (auto support_batch_delete = s3_capabilities.isBatchDeleteSupported();
                 support_batch_delete.has_value() && !support_batch_delete.value())
            try_batch_delete = false; /// Support for batch delete is disabled.
    }

    auto log = getLogger("deleteFileFromS3");
    const String empty_string;

    if (try_batch_delete)
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
            delkeys.SetQuiet(true);

            S3::DeleteObjectsRequest request;
            request.SetBucket(bucket);
            request.SetDelete(delkeys);

            ProfileEvents::increment(ProfileEvents::S3DeleteObjects);
            if (profile_event && *profile_event != ProfileEvents::S3DeleteObjects)
                ProfileEvents::increment(*profile_event);

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

            if (outcome.IsSuccess())
            {
                /// DeleteObjects succeeded, that means some objects were removed (but maybe not all the objects).
                /// Multiple threads can call deleteFilesFromS3() with a reference to the same `s3_capabilities`,
                /// and the following line doesn't cause a race because `s3_capabilities` is protected with mutex.
                s3_capabilities.setIsBatchDeleteSupported(true);

                const auto & errors = outcome.GetResult().GetErrors();
                if (errors.empty())
                {
                    /// All the objects were removed.
                    LOG_DEBUG(log, "Objects with paths [{}] were removed from S3", comma_separated_keys);
                }
                else
                {
                    /// Mixed success/error response - some objects were removed, and some were not.
                    /// We need to extract more detailed information from the outcome.
                    std::unordered_set<std::string_view> removed_keys{keys.begin(), keys.end()};
                    String not_found_keys;
                    std::exception_ptr other_error;

                    for (const auto & err : errors)
                    {
                        removed_keys.erase(err.GetKey());
                        auto error_type = static_cast<Aws::S3::S3Errors>(Aws::S3::S3ErrorMapper::GetErrorForName(err.GetCode().c_str()).GetErrorType());
                        if (if_exists && S3::isNotFoundError(error_type))
                        {
                            if (not_found_keys.empty())
                                not_found_keys += ", ";
                            not_found_keys += err.GetKey();
                        }
                        else if (!other_error)
                        {
                            other_error = std::make_exception_ptr(
                                S3Exception{error_type, "{} (Code: {}) while removing object with path {} from S3",
                                            err.GetMessage(), err.GetCode(), err.GetKey()});
                        }
                    }

                    if (!removed_keys.empty())
                    {
                        String removed_keys_comma_separated;
                        for (const auto & key : removed_keys)
                        {
                            if (!removed_keys_comma_separated.empty())
                                removed_keys_comma_separated += ", ";
                            removed_keys_comma_separated += key;
                        }
                        LOG_DEBUG(log, "Objects with paths [{}] were removed from S3", removed_keys_comma_separated);
                    }

                    if (!not_found_keys.empty())
                    {
                        /// In this case even if absence of key may be ok for us, the log will be polluted with error messages from aws sdk.
                        /// Looks like there is no way to suppress them.
                        LOG_TRACE(log, "Object with paths [{}] were skipped because they didn't exist", not_found_keys);
                    }

                    if (other_error)
                        std::rethrow_exception(other_error);
                }
            }
            else
            {
                /// DeleteObjects didn't succeed, that means either a) this operation isn't supported at all;
                /// or b) all the objects didn't exist; or c) some failure occurred.
                const auto & err = outcome.GetError();
                if ((err.GetExceptionName() == "InvalidRequest") || (err.GetExceptionName() == "InvalidArgument")
                    || (err.GetExceptionName() == "NotImplemented"))
                {
                    LOG_TRACE(log, "DeleteObjects is not supported: {} (Code: {}). Retrying with plain DeleteObject.",
                              err.GetMessage(), static_cast<size_t>(err.GetErrorType()));
                    /// Multiple threads can call deleteFilesFromS3() with a reference to the same `s3_capabilities`,
                    /// and the following line doesn't cause a race because `s3_capabilities` is protected with mutex.
                    s3_capabilities.setIsBatchDeleteSupported(false);
                    need_retry_with_plain_delete_object = true;
                    break;
                }

                if (if_exists && S3::isNotFoundError(err.GetErrorType()))
                {
                    LOG_TRACE(log, "Object with paths [{}] were skipped because they didn't exist", comma_separated_keys);
                }
                else
                {
                    throw S3Exception(err.GetErrorType(), "{} (Code: {}) while removing objects with paths [{}] from S3",
                                      err.GetMessage(), static_cast<size_t>(err.GetErrorType()), comma_separated_keys);
                }
            }
        }

        if (!need_retry_with_plain_delete_object)
            return;
    }

    /// Batch delete (DeleteObjects) isn't supported so we'll delete all the files sequentially.
    for (size_t i = 0; i != keys.size(); ++i)
    {
        const String & local_path_for_blob_storage_log = (i < local_paths_for_blob_storage_log.size()) ? local_paths_for_blob_storage_log[i] : empty_string;
        size_t file_size_for_blob_storage_log = (i < file_sizes_for_blob_storage_log.size()) ? file_sizes_for_blob_storage_log[i] : 0;

        deleteFileFromS3(s3_client, bucket, keys[i], if_exists,
                         blob_storage_log, local_path_for_blob_storage_log, file_size_for_blob_storage_log,
                         profile_event);
    }
}

}

#endif
