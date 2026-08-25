#pragma once

#include <Common/IThrottler.h>
#include <Common/Scheduler/ResourceLink.h>
#include <IO/DistributedCacheSettings.h>

#include <optional>

namespace DB
{

/// Per-write retry-behavior selector, resolved by the object storage that executes the write.
/// SingleAttempt: exactly one HTTP attempt, no SDK-transparent retries — for conditional writes
/// whose retry loop lives above the storage client (it must resolve an uncertain PUT before
/// reissuing). Backends without a SingleAttempt implementation report it via
/// IObjectStorage::supportsRetryProfile; writers must fail closed rather than fall through.
enum class ObjectStorageRetryProfile : uint8_t
{
    Default,
    SingleAttempt,
};

/// Per-request GCS conditional-dialect opt-in, carried alongside the write itself so it survives
/// into the object storage request that ends up on the wire (see `RequestWithNativeConditionalMode`).
/// NativeConditional: this write is content-addressed-storage-owned and may use GCS generation
/// tokens instead of the AWS-style ETag plumbing, when the client's HTTP layer supports it.
enum class ObjectStorageRequestMode : uint8_t
{
    Default,
    NativeConditional,
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
    /// override). Writers of CAS-MUTABLE keys (content-addressed shard manifests) set `false`:
    /// such a key is legitimately replaced by a concurrent conditional PUT between this upload and
    /// the check's HEAD, so the size comparison false-positives ("it's a bug in S3") under normal
    /// contention. Integrity for those keys is the conditional PUT outcome + token, not a recheck.
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
    /// GCS enforces no preconditions on CompleteMultipartUpload (measured 2026-07-03). When set,
    /// WriteBufferFromS3 throws instead of starting a multipart upload.
    bool s3_force_single_part_upload = false;
    /// Companion cap: raises max_single_part_upload_size / min_upload_part_size in the request
    /// settings so bodies up to this size stay in ONE part (RAM-buffered). 0 = no override.
    size_t s3_single_part_upload_max_bytes_override = 0;

    /// Overrides S3RequestSetting::max_unexpected_write_error_retries (default 4) for this write.
    /// WriteBufferFromS3::makeSinglepartUpload/completeMultipartUpload run their OWN retry loop above
    /// the S3 client that reissues the identical request (WITH its If-None-Match/If-Match condition)
    /// on a NO_SUCH_KEY response — a second retry-affecting layer a client-level override
    /// (a client-level profile override) does not reach. A CAS conditional write sets this to 1 for
    /// exactly one attempt at this layer too (RFC cas-s3-timeout-retry-control). 0 = no override.
    size_t s3_max_unexpected_write_error_retries_override = 0;

    /// Selects the retry profile the object storage should execute this write under; see
    /// ObjectStorageRetryProfile.
    ObjectStorageRetryProfile object_storage_retry_profile = ObjectStorageRetryProfile::Default;

    /// Selects the object storage request mode this write should carry; see ObjectStorageRequestMode.
    ObjectStorageRequestMode object_storage_request_mode = ObjectStorageRequestMode::Default;

    bool operator==(const WriteSettings & other) const = default;
};

WriteSettings getWriteSettings();

WriteSettings getWriteSettingsForMetadata();
}
