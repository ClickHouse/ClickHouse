#pragma once

#include <Common/IThrottler.h>
#include <Common/Scheduler/ResourceLink.h>
#include <IO/DistributedCacheSettings.h>
#include <IO/ObjectStorageRequestMode.h>
#include <IO/ObjectStorageRequestProfile.h>

#include <optional>

namespace DB
{

/// Per-copy transport requirement, resolved by the object storage that executes the copy.
/// `NativeOnly` requires a provider-native same-store copy and forbids a client-side fallback.
enum class ObjectStorageCopyMode : uint8_t
{
    Default,
    NativeOnly,
};

/// Settings to be passed to IDisk::writeFile()
struct WriteSettings
{
    /// Bandwidth throttler to use during writing
    ThrottlerPtr remote_throttler;
    ThrottlerPtr local_throttler;

    IOSchedulingSettings io_scheduling;

    /// Filesystem cache settings
    bool enable_filesystem_cache_on_write_operations = false;
    bool enable_filesystem_cache_log = false;
    bool throw_on_error_from_cache = false;
    size_t filesystem_cache_reserve_space_wait_lock_timeout_milliseconds = 1000;

    bool s3_allow_parallel_part_upload = true;
    /// Overrides S3RequestSetting::check_objects_after_upload for this write (nullopt = no
    /// override). A writer whose key can legitimately be replaced by a concurrent conditional PUT
    /// between this upload and the check's HEAD sets `false`: otherwise the size comparison
    /// false-positives ("it's a bug in S3") under normal contention. Integrity for such a key comes
    /// from the conditional PUT outcome and token, not a recheck.
    std::optional<bool> s3_check_objects_after_upload_override;
    bool azure_allow_parallel_part_upload = true;

    bool use_adaptive_write_buffer = false;
    size_t adaptive_write_buffer_initial_size = 16 * 1024;

    bool write_through_distributed_cache = false;
    DistributedCacheSettings distributed_cache_settings;

    bool is_initial_access_check = false;

    std::string object_storage_write_if_none_match; /// Supported only for S3-like object storages.
    std::string object_storage_write_if_match;     /// Supported only for S3-like object storages.

    /// A conditional write on a generation-token store (GCS) must never take the multipart path:
    /// GCS enforces no preconditions on CompleteMultipartUpload (measured 2026-07-03). The size
    /// ceiling for this write comes from the object storage's own `gcs_max_conditional_put_bytes`.
    bool s3_force_single_part_upload = false;

    /// Overrides S3RequestSetting::max_unexpected_write_error_retries (default 4) for this write.
    /// WriteBufferFromS3::makeSinglepartUpload/completeMultipartUpload run their OWN retry loop above
    /// the S3 client that reissues the identical request (WITH its If-None-Match/If-Match condition)
    /// on a NO_SUCH_KEY response — a second retry-affecting layer a client-level profile override does
    /// not reach. A conditional write that must not retry at that layer either sets this to 1 for
    /// exactly one attempt. 0 = no override.
    size_t s3_max_unexpected_write_error_retries_override = 0;

    /// Selects the retry profile the object storage should execute this write under; see
    /// ObjectStorageRetryProfile.
    ObjectStorageRetryProfile object_storage_retry_profile = ObjectStorageRetryProfile::Default;

    /// Request timeout (send/receive inactivity bound) for the single-attempt client selected by
    /// `object_storage_retry_profile == SingleAttempt`. 0 = the storage's configured timeout.
    uint64_t object_storage_attempt_timeout_ms = 0;

    /// The cap the single-attempt client's clone puts on one TCP connect and again on one TLS
    /// handshake, frozen by the mount at open; see `CasRequestBudget::attemptEnvelopeMs`. 0 = no cap.
    uint64_t object_storage_connect_timeout_cap_ms = 0;

    /// The caller's own attempt number for the request built from these settings, 1-based; 0 leaves the
    /// buffer's own numbering. A caller reissuing this write passes its count so the HTTP client sees
    /// attempt ≥ 2.
    size_t object_storage_attempt_number = 0;

    /// Selects the transport requirement for an object storage copy; see `ObjectStorageCopyMode`.
    ObjectStorageCopyMode object_storage_copy_mode = ObjectStorageCopyMode::Default;

    /// Selects the object storage request mode this write should carry; see ObjectStorageRequestMode.
    ObjectStorageRequestMode object_storage_request_mode = ObjectStorageRequestMode::Default;

    bool operator==(const WriteSettings & other) const = default;
};

WriteSettings getWriteSettings();

WriteSettings getWriteSettingsForMetadata();
}
