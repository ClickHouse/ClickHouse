#include <IO/GCS/GCSClient.h>

#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>

#include <algorithm>
#include <chrono>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#if USE_GOOGLE_CLOUD
#    include <google/cloud/credentials.h>
#    include <google/cloud/storage/grpc_plugin.h>
#    include <google/cloud/storage/download_options.h>
#    include <google/cloud/storage/object_metadata.h>
#    include <google/cloud/storage/retry_policy.h>
#endif

namespace DB::ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}

namespace ProfileEvents
{
extern const Event GCSGetObject;
extern const Event GCSListObjects;
extern const Event GCSDeleteObject;
extern const Event GCSReadObject;
extern const Event GCSWriteObject;
extern const Event DiskGCSGetObject;
extern const Event DiskGCSListObjects;
extern const Event DiskGCSDeleteObject;
extern const Event DiskGCSReadObject;
extern const Event DiskGCSWriteObject;
extern const Event GCSReadMicroseconds;
extern const Event GCSReadRequestsCount;
extern const Event GCSReadRequestsErrors;
extern const Event GCSReadRequestsThrottling;
extern const Event GCSReadRequestAttempts;
extern const Event GCSReadRequestRetryableErrors;
extern const Event GCSWriteMicroseconds;
extern const Event GCSWriteRequestsCount;
extern const Event GCSWriteRequestsErrors;
extern const Event GCSWriteRequestsThrottling;
extern const Event GCSWriteRequestAttempts;
extern const Event GCSWriteRequestRetryableErrors;
extern const Event DiskGCSReadMicroseconds;
extern const Event DiskGCSReadRequestsCount;
extern const Event DiskGCSReadRequestsErrors;
extern const Event DiskGCSReadRequestsThrottling;
extern const Event DiskGCSReadRequestAttempts;
extern const Event DiskGCSReadRequestRetryableErrors;
extern const Event DiskGCSWriteMicroseconds;
extern const Event DiskGCSWriteRequestsCount;
extern const Event DiskGCSWriteRequestsErrors;
extern const Event DiskGCSWriteRequestsThrottling;
extern const Event DiskGCSWriteRequestAttempts;
extern const Event DiskGCSWriteRequestRetryableErrors;
extern const Event GCSGetRequestThrottlerBlocked;
extern const Event GCSPutRequestThrottlerBlocked;
extern const Event DiskGCSGetRequestThrottlerCount;
extern const Event DiskGCSGetRequestThrottlerBlocked;
extern const Event DiskGCSGetRequestThrottlerSleepMicroseconds;
extern const Event DiskGCSPutRequestThrottlerCount;
extern const Event DiskGCSPutRequestThrottlerBlocked;
extern const Event DiskGCSPutRequestThrottlerSleepMicroseconds;
}

namespace DB::GCS
{

CredentialMode credentialMode(const ClientSettings & settings)
{
    if (settings.use_insecure_credentials_for_tests)
        return CredentialMode::InsecureForTests;
    if (!settings.service_account_json.empty())
        return CredentialMode::ServiceAccountKey;
    return CredentialMode::GoogleDefault;
}

const char * credentialModeName(CredentialMode mode)
{
    switch (mode)
    {
        case CredentialMode::GoogleDefault:
            return "google_default";
        case CredentialMode::ServiceAccountKey:
            return "service_account_key";
        case CredentialMode::InsecureForTests:
            return "insecure_for_tests";
    }
}

bool isGrpcAvailable()
{
    return USE_GOOGLE_CLOUD;
}

void assertGrpcAvailable()
{
#if !USE_GOOGLE_CLOUD
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED,
        "Native GCS gRPC support is not available because ClickHouse was built without Google Cloud C++ gRPC support");
#endif
}

#if USE_GOOGLE_CLOUD
namespace
{

struct OperationEvents
{
    ProfileEvents::Event operation;
    ProfileEvents::Event disk_operation;
    ProfileEvents::Event microseconds;
    ProfileEvents::Event count;
    ProfileEvents::Event errors;
    ProfileEvents::Event throttling;
    ProfileEvents::Event attempts;
    ProfileEvents::Event retryable_errors;
    ProfileEvents::Event disk_microseconds;
    ProfileEvents::Event disk_count;
    ProfileEvents::Event disk_errors;
    ProfileEvents::Event disk_throttling;
    ProfileEvents::Event disk_attempts;
    ProfileEvents::Event disk_retryable_errors;
    bool use_get_throttler;
    bool retry_stream_creation;
};

const OperationEvents get_object_events{
    ProfileEvents::GCSGetObject,
    ProfileEvents::DiskGCSGetObject,
    ProfileEvents::GCSReadMicroseconds,
    ProfileEvents::GCSReadRequestsCount,
    ProfileEvents::GCSReadRequestsErrors,
    ProfileEvents::GCSReadRequestsThrottling,
    ProfileEvents::GCSReadRequestAttempts,
    ProfileEvents::GCSReadRequestRetryableErrors,
    ProfileEvents::DiskGCSReadMicroseconds,
    ProfileEvents::DiskGCSReadRequestsCount,
    ProfileEvents::DiskGCSReadRequestsErrors,
    ProfileEvents::DiskGCSReadRequestsThrottling,
    ProfileEvents::DiskGCSReadRequestAttempts,
    ProfileEvents::DiskGCSReadRequestRetryableErrors,
    true,
    false};

const OperationEvents list_objects_events{
    ProfileEvents::GCSListObjects,
    ProfileEvents::DiskGCSListObjects,
    ProfileEvents::GCSWriteMicroseconds,
    ProfileEvents::GCSWriteRequestsCount,
    ProfileEvents::GCSWriteRequestsErrors,
    ProfileEvents::GCSWriteRequestsThrottling,
    ProfileEvents::GCSWriteRequestAttempts,
    ProfileEvents::GCSWriteRequestRetryableErrors,
    ProfileEvents::DiskGCSWriteMicroseconds,
    ProfileEvents::DiskGCSWriteRequestsCount,
    ProfileEvents::DiskGCSWriteRequestsErrors,
    ProfileEvents::DiskGCSWriteRequestsThrottling,
    ProfileEvents::DiskGCSWriteRequestAttempts,
    ProfileEvents::DiskGCSWriteRequestRetryableErrors,
    false,
    false};

const OperationEvents delete_object_events{
    ProfileEvents::GCSDeleteObject,
    ProfileEvents::DiskGCSDeleteObject,
    ProfileEvents::GCSWriteMicroseconds,
    ProfileEvents::GCSWriteRequestsCount,
    ProfileEvents::GCSWriteRequestsErrors,
    ProfileEvents::GCSWriteRequestsThrottling,
    ProfileEvents::GCSWriteRequestAttempts,
    ProfileEvents::GCSWriteRequestRetryableErrors,
    ProfileEvents::DiskGCSWriteMicroseconds,
    ProfileEvents::DiskGCSWriteRequestsCount,
    ProfileEvents::DiskGCSWriteRequestsErrors,
    ProfileEvents::DiskGCSWriteRequestsThrottling,
    ProfileEvents::DiskGCSWriteRequestAttempts,
    ProfileEvents::DiskGCSWriteRequestRetryableErrors,
    false,
    false};

const OperationEvents read_object_events{
    ProfileEvents::GCSReadObject,
    ProfileEvents::DiskGCSReadObject,
    ProfileEvents::GCSReadMicroseconds,
    ProfileEvents::GCSReadRequestsCount,
    ProfileEvents::GCSReadRequestsErrors,
    ProfileEvents::GCSReadRequestsThrottling,
    ProfileEvents::GCSReadRequestAttempts,
    ProfileEvents::GCSReadRequestRetryableErrors,
    ProfileEvents::DiskGCSReadMicroseconds,
    ProfileEvents::DiskGCSReadRequestsCount,
    ProfileEvents::DiskGCSReadRequestsErrors,
    ProfileEvents::DiskGCSReadRequestsThrottling,
    ProfileEvents::DiskGCSReadRequestAttempts,
    ProfileEvents::DiskGCSReadRequestRetryableErrors,
    true,
    true};

const OperationEvents write_object_events{
    ProfileEvents::GCSWriteObject,
    ProfileEvents::DiskGCSWriteObject,
    ProfileEvents::GCSWriteMicroseconds,
    ProfileEvents::GCSWriteRequestsCount,
    ProfileEvents::GCSWriteRequestsErrors,
    ProfileEvents::GCSWriteRequestsThrottling,
    ProfileEvents::GCSWriteRequestAttempts,
    ProfileEvents::GCSWriteRequestRetryableErrors,
    ProfileEvents::DiskGCSWriteMicroseconds,
    ProfileEvents::DiskGCSWriteRequestsCount,
    ProfileEvents::DiskGCSWriteRequestsErrors,
    ProfileEvents::DiskGCSWriteRequestsThrottling,
    ProfileEvents::DiskGCSWriteRequestAttempts,
    ProfileEvents::DiskGCSWriteRequestRetryableErrors,
    false,
    true};

Status statusFromCloud(const google::cloud::Status & status)
{
    if (status.ok())
        return {};

    switch (status.code())
    {
        case google::cloud::StatusCode::kNotFound:
            return makeStatus(StatusCode::NotFound, status.message());
        case google::cloud::StatusCode::kPermissionDenied:
        case google::cloud::StatusCode::kUnauthenticated:
            return makeStatus(StatusCode::PermissionDenied, status.message());
        case google::cloud::StatusCode::kDeadlineExceeded:
            return makeStatus(StatusCode::DeadlineExceeded, status.message());
        case google::cloud::StatusCode::kResourceExhausted:
            return makeStatus(StatusCode::ResourceExhausted, status.message());
        case google::cloud::StatusCode::kUnavailable:
            return makeStatus(StatusCode::Unavailable, status.message());
        case google::cloud::StatusCode::kInvalidArgument:
        case google::cloud::StatusCode::kFailedPrecondition:
        case google::cloud::StatusCode::kOutOfRange:
            return makeStatus(StatusCode::InvalidArgument, status.message());
        case google::cloud::StatusCode::kUnimplemented:
            return makeStatus(StatusCode::Unsupported, status.message());
        default:
            return makeStatus(StatusCode::Unknown, status.message());
    }
}

void configureRequestThrottlerEvents(ClientSettings & settings)
{
    settings.request_throttler.get_blocked = ProfileEvents::GCSGetRequestThrottlerBlocked;
    settings.request_throttler.put_blocked = ProfileEvents::GCSPutRequestThrottlerBlocked;

    if (settings.for_disk)
    {
        settings.request_throttler.disk_get_amount = ProfileEvents::DiskGCSGetRequestThrottlerCount;
        settings.request_throttler.disk_get_blocked = ProfileEvents::DiskGCSGetRequestThrottlerBlocked;
        settings.request_throttler.disk_get_sleep_us = ProfileEvents::DiskGCSGetRequestThrottlerSleepMicroseconds;
        settings.request_throttler.disk_put_amount = ProfileEvents::DiskGCSPutRequestThrottlerCount;
        settings.request_throttler.disk_put_blocked = ProfileEvents::DiskGCSPutRequestThrottlerBlocked;
        settings.request_throttler.disk_put_sleep_us = ProfileEvents::DiskGCSPutRequestThrottlerSleepMicroseconds;
    }
}

UInt64 maxAttempts(const ClientSettings & settings)
{
    return std::max<UInt64>(settings.max_retry_attempts, 1);
}

void throttleRequest(const ClientSettings & settings, const OperationEvents & events)
{
    if (events.use_get_throttler)
        settings.request_throttler.throttleHTTPGet();
    else
        settings.request_throttler.throttleHTTPPut();
}

void recordAttemptStart(const OperationEvents & events, bool for_disk)
{
    ProfileEvents::increment(events.operation);
    ProfileEvents::increment(events.count);
    ProfileEvents::increment(events.attempts);

    if (for_disk)
    {
        ProfileEvents::increment(events.disk_operation);
        ProfileEvents::increment(events.disk_count);
        ProfileEvents::increment(events.disk_attempts);
    }
}

void recordAttemptTime(const OperationEvents & events, bool for_disk, UInt64 elapsed_microseconds)
{
    ProfileEvents::increment(events.microseconds, elapsed_microseconds);
    if (for_disk)
        ProfileEvents::increment(events.disk_microseconds, elapsed_microseconds);
}

void recordFailure(const OperationEvents & events, bool for_disk, StatusCode code)
{
    if (isThrottlingStatus(code))
    {
        ProfileEvents::increment(events.throttling);
        if (for_disk)
            ProfileEvents::increment(events.disk_throttling);
    }
    else
    {
        ProfileEvents::increment(events.errors);
        if (for_disk)
            ProfileEvents::increment(events.disk_errors);
    }

    if (isRetryableStatus(code))
    {
        ProfileEvents::increment(events.retryable_errors);
        if (for_disk)
            ProfileEvents::increment(events.disk_retryable_errors);
    }
}

bool shouldRetry(const Status & status, UInt64 attempt, UInt64 max_attempts)
{
    return !status.ok() && attempt < max_attempts && isRetryableStatus(status.code);
}

google::cloud::storage::ObjectMetadata makeCloudObjectMetadata(const std::map<std::string, std::string> & metadata)
{
    google::cloud::storage::ObjectMetadata object_metadata;
    for (const auto & [key, value] : metadata)
        object_metadata.upsert_metadata(key, value);
    return object_metadata;
}

template <typename Response, typename Call>
Result<Response> executeHighLevelStatusOrRequest(const ClientSettings & settings, const OperationEvents & events, Call && call)
{
    Result<Response> result;
    const UInt64 attempts = maxAttempts(settings);
    for (UInt64 attempt = 1; attempt <= attempts; ++attempt)
    {
        throttleRequest(settings, events);
        recordAttemptStart(events, settings.for_disk);

        Stopwatch watch;
        auto response = call();
        recordAttemptTime(events, settings.for_disk, watch.elapsedMicroseconds());

        if (response)
        {
            result.response = *std::move(response);
            result.status = {};
            return result;
        }

        result.status = fromCloudStatus(response.status());
        recordFailure(events, settings.for_disk, result.status.code);
        if (!shouldRetry(result.status, attempt, attempts))
            return result;
    }
    return result;
}

template <typename Call>
Status executeHighLevelStatusRequest(const ClientSettings & settings, const OperationEvents & events, Call && call)
{
    Status status;
    const UInt64 attempts = maxAttempts(settings);
    for (UInt64 attempt = 1; attempt <= attempts; ++attempt)
    {
        throttleRequest(settings, events);
        recordAttemptStart(events, settings.for_disk);

        Stopwatch watch;
        status = fromCloudStatus(call());
        recordAttemptTime(events, settings.for_disk, watch.elapsedMicroseconds());
        if (status.ok())
            return status;

        recordFailure(events, settings.for_disk, status.code);
        if (!shouldRetry(status, attempt, attempts))
            return status;
    }
    return status;
}

std::shared_ptr<google::cloud::Credentials> makeCredentials(const ClientSettings & settings)
{
    switch (credentialMode(settings))
    {
        case CredentialMode::InsecureForTests:
            return google::cloud::MakeInsecureCredentials();
        case CredentialMode::ServiceAccountKey:
            return google::cloud::MakeServiceAccountCredentials(settings.service_account_json);
        case CredentialMode::GoogleDefault:
            return google::cloud::MakeGoogleDefaultCredentials();
    }
}


}

Status fromCloudStatus(const google::cloud::Status & status)
{
    return statusFromCloud(status);
}

google::cloud::Options makeGrpcClientOptions(const ClientSettings & settings)
{
    assertGrpcAvailable();

    google::cloud::Options options;
    if (!settings.endpoint.empty())
        options.set<google::cloud::EndpointOption>(settings.endpoint);

    options.set<google::cloud::UnifiedCredentialsOption>(makeCredentials(settings));

    if (!settings.user_project.empty())
        options.set<google::cloud::UserProjectOption>(settings.user_project);

    options.set<google::cloud::storage::RetryPolicyOption>(
        google::cloud::storage::LimitedErrorCountRetryPolicy(0).clone());

    const auto request_timeout = std::chrono::milliseconds(settings.request_timeout_ms);
    if (request_timeout > std::chrono::milliseconds::zero())
        options.set<google::cloud::storage::TransferStallTimeoutOption>(
            std::chrono::ceil<std::chrono::seconds>(request_timeout));

    return options;
}

HighLevelClient::HighLevelClient(ClientSettings settings_, google::cloud::Options options_, google::cloud::storage::Client client_)
    : settings(std::move(settings_))
    , options(std::move(options_))
    , client(std::move(client_))
{
    configureRequestThrottlerEvents(settings);
}

HighLevelReadResult HighLevelClient::readObject(
    const std::string & bucket, const std::string & object, size_t offset, std::optional<size_t> limit)
{
    HighLevelReadResult result;
    const UInt64 attempts = maxAttempts(settings);
    for (UInt64 attempt = 1; attempt <= attempts; ++attempt)
    {
        throttleRequest(settings, read_object_events);
        recordAttemptStart(read_object_events, settings.for_disk);

        Stopwatch watch;
        if (limit)
        {
            result.stream = client.ReadObject(
                bucket,
                object,
                google::cloud::storage::ReadRange(
                    static_cast<std::int64_t>(offset), static_cast<std::int64_t>(offset + *limit)));
        }
        else
        {
            result.stream = client.ReadObject(bucket, object, google::cloud::storage::ReadFromOffset(static_cast<std::int64_t>(offset)));
        }
        recordAttemptTime(read_object_events, settings.for_disk, watch.elapsedMicroseconds());

        result.status = fromCloudStatus(result.stream.status());
        if (result.status.ok())
            return result;

        recordFailure(read_object_events, settings.for_disk, result.status.code);
        if (!shouldRetry(result.status, attempt, attempts))
            return result;
    }

    return result;
}

void HighLevelClient::recordReadObjectFailure(const Status & status) const
{
    if (!status.ok())
        recordFailure(read_object_events, settings.for_disk, status.code);
}

Result<google::cloud::storage::ObjectMetadata> HighLevelClient::insertObject(
    const std::string & bucket,
    const std::string & object,
    std::string_view payload,
    const std::map<std::string, std::string> & metadata,
    bool if_generation_match_zero)
{
    auto payload_view = absl::string_view(payload.data(), payload.size());
    return executeHighLevelStatusOrRequest<google::cloud::storage::ObjectMetadata>(
        settings,
        write_object_events,
        [&]
        {
            auto object_metadata = makeCloudObjectMetadata(metadata);
            if (if_generation_match_zero && !metadata.empty())
                return client.InsertObject(
                    bucket,
                    object,
                    payload_view,
                    google::cloud::storage::IfGenerationMatch(0),
                    google::cloud::storage::WithObjectMetadata(std::move(object_metadata)));
            if (if_generation_match_zero)
                return client.InsertObject(bucket, object, payload_view, google::cloud::storage::IfGenerationMatch(0));
            if (!metadata.empty())
                return client.InsertObject(bucket, object, payload_view, google::cloud::storage::WithObjectMetadata(std::move(object_metadata)));
            return client.InsertObject(bucket, object, payload_view);
        });
}

Result<google::cloud::storage::ObjectMetadata> HighLevelClient::composeObject(
    const std::string & bucket,
    const std::vector<std::string> & sources,
    const std::string & destination,
    const std::map<std::string, std::string> & metadata,
    bool if_generation_match_zero)
{
    std::vector<google::cloud::storage::ComposeSourceObject> source_objects;
    source_objects.reserve(sources.size());
    for (const auto & source : sources)
        source_objects.push_back(google::cloud::storage::ComposeSourceObject{source, {}, {}});

    return executeHighLevelStatusOrRequest<google::cloud::storage::ObjectMetadata>(
        settings,
        write_object_events,
        [&]
        {
            auto object_metadata = makeCloudObjectMetadata(metadata);
            if (if_generation_match_zero && !metadata.empty())
                return client.ComposeObject(
                    bucket,
                    source_objects,
                    destination,
                    google::cloud::storage::IfGenerationMatch(0),
                    google::cloud::storage::WithObjectMetadata(std::move(object_metadata)));
            if (if_generation_match_zero)
                return client.ComposeObject(bucket, source_objects, destination, google::cloud::storage::IfGenerationMatch(0));
            if (!metadata.empty())
                return client.ComposeObject(bucket, source_objects, destination, google::cloud::storage::WithObjectMetadata(std::move(object_metadata)));
            return client.ComposeObject(bucket, source_objects, destination);
        });
}

Result<google::cloud::storage::ObjectMetadata> HighLevelClient::rewriteObject(
    const std::string & source_bucket,
    const std::string & source_object,
    const std::string & destination_bucket,
    const std::string & destination_object,
    const std::map<std::string, std::string> & metadata,
    bool if_generation_match_zero)
{
    return executeHighLevelStatusOrRequest<google::cloud::storage::ObjectMetadata>(
        settings,
        write_object_events,
        [&]
        {
            auto object_metadata = makeCloudObjectMetadata(metadata);
            if (if_generation_match_zero && !metadata.empty())
                return client.RewriteObjectBlocking(
                    source_bucket,
                    source_object,
                    destination_bucket,
                    destination_object,
                    google::cloud::storage::IfGenerationMatch(0),
                    google::cloud::storage::WithObjectMetadata(std::move(object_metadata)));
            if (if_generation_match_zero)
                return client.RewriteObjectBlocking(
                    source_bucket, source_object, destination_bucket, destination_object, google::cloud::storage::IfGenerationMatch(0));
            if (!metadata.empty())
                return client.RewriteObjectBlocking(
                    source_bucket,
                    source_object,
                    destination_bucket,
                    destination_object,
                    google::cloud::storage::WithObjectMetadata(std::move(object_metadata)));
            return client.RewriteObjectBlocking(source_bucket, source_object, destination_bucket, destination_object);
        });
}

Result<google::cloud::storage::ObjectMetadata> HighLevelClient::getObjectMetadata(
    const std::string & bucket, const std::string & object)
{
    return executeHighLevelStatusOrRequest<google::cloud::storage::ObjectMetadata>(
        settings, get_object_events, [&] { return client.GetObjectMetadata(bucket, object); });
}

Result<std::vector<google::cloud::storage::ObjectMetadata>> HighLevelClient::listObjects(
    const std::string & bucket, const std::string & prefix, size_t max_keys, const std::optional<std::string> & start_after)
{
    Result<std::vector<google::cloud::storage::ObjectMetadata>> result;
    const UInt64 attempts = maxAttempts(settings);
    const auto page_size = max_keys ? max_keys : 1000;

    for (UInt64 attempt = 1; attempt <= attempts; ++attempt)
    {
        result.response.clear();
        result.status = {};
        bool retry = false;
        throttleRequest(settings, list_objects_events);
        recordAttemptStart(list_objects_events, settings.for_disk);

        Stopwatch watch;
        auto reader = [&]
        {
            if (start_after && !start_after->empty())
                return client.ListObjects(
                    bucket,
                    google::cloud::storage::Prefix(prefix),
                    google::cloud::storage::MaxResults(static_cast<std::int64_t>(page_size)),
                    google::cloud::storage::StartOffset(*start_after));
            return client.ListObjects(
                bucket, google::cloud::storage::Prefix(prefix), google::cloud::storage::MaxResults(static_cast<std::int64_t>(page_size)));
        }();

        for (auto && object : reader)
        {
            if (!object)
            {
                result.status = fromCloudStatus(object.status());
                recordAttemptTime(list_objects_events, settings.for_disk, watch.elapsedMicroseconds());
                recordFailure(list_objects_events, settings.for_disk, result.status.code);
                retry = shouldRetry(result.status, attempt, attempts);
                break;
            }
            result.response.push_back(*std::move(object));
            if (max_keys && result.response.size() >= max_keys)
                break;
        }

        if (retry)
            continue;
        if (!result.status.ok())
            return result;

        recordAttemptTime(list_objects_events, settings.for_disk, watch.elapsedMicroseconds());
        result.status = {};
        return result;
    }

    return result;
}

Status HighLevelClient::deleteObject(const std::string & bucket, const std::string & object)
{
    return executeHighLevelStatusRequest(settings, delete_object_events, [&] { return client.DeleteObject(bucket, object); });
}

void HighLevelClient::recordWriteObjectFailure(const Status & status) const
{
    if (!status.ok())
        recordFailure(write_object_events, settings.for_disk, status.code);
}

std::shared_ptr<HighLevelClient> createHighLevelClient(const ClientSettings & settings)
{
    auto options = makeGrpcClientOptions(settings);
    auto client = google::cloud::storage::MakeGrpcClient(options);
    return std::make_shared<HighLevelClient>(settings, std::move(options), std::move(client));
}

#endif
}
