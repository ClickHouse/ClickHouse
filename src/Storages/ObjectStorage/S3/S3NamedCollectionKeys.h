#pragma once

#include <string_view>
#include <unordered_set>

namespace DB
{

/// The keys of an S3 named collection, with its overrides, that `S3StorageParsedArguments::fromNamedCollection` accepts.
/// The masking of query arguments (`S3SecretArguments.cpp`) hides the value of an override with any other key, so both
/// use these sets. This header does not depend on `USE_AWS_S3`, because the masking is built without it too.
inline const std::unordered_set<std::string_view> s3_named_collection_required_keys =
{
    "url",
};

inline const std::unordered_set<std::string_view> s3_named_collection_optional_keys =
{
    "format",
    "compression",
    "compression_method",
    "structure",
    "access_key_id",
    "secret_access_key",
    "session_token",
    "filename",
    "use_environment_credentials",
    "max_single_read_retries",
    "min_upload_part_size",
    "upload_part_size_multiply_factor",
    "upload_part_size_multiply_parts_count_threshold",
    "max_single_part_upload_size",
    "max_connections",
    "expiration_window_seconds",
    "no_sign_request",
    "partition_strategy",
    "partition_columns_in_data_file",
    "storage_class_name",
    "storage_class", /// Interchangeable alias for `storage_class_name`, see issue #68551
    "upload_checksum_algorithm",
    "use_idempotency_id",
    /// Private configuration options
    "role_arn", /// for extra_credentials
    "role_session_name", /// for extra_credentials
    "external_id", /// for extra_credentials
    "http_client", /// For GCP
    "metadata_service", /// For GCP
    "service_account", /// For GCP
    "request_token_path", /// For GCP
    "google_adc_client_id", /// For GCP (explicit Application Default Credentials triple)
    "google_adc_client_secret", /// For GCP
    "google_adc_refresh_token", /// For GCP
};

inline bool isS3NamedCollectionKey(std::string_view key)
{
    return s3_named_collection_required_keys.contains(key) || s3_named_collection_optional_keys.contains(key);
}

}
