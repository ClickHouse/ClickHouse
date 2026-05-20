#include <IO/GCS/GCSClient.h>

#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>

#include <algorithm>
#include <limits>
#include <string>
#include <string_view>
#include <vector>
#if USE_GOOGLE_CLOUD
#    include <absl/strings/cord.h>
#    include <google/cloud/completion_queue.h>
#    include <google/cloud/credentials.h>
#    include <google/cloud/internal/unified_grpc_credentials.h>
#    include <google/cloud/internal/url_encode.h>
#    include <grpcpp/test/client_context_test_peer.h>
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

Status fromCloudStatus(const google::cloud::Status & status)
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

template <typename Response, typename Request, typename Call>
Result<Response> executeUnaryRequest(
    const Client & client,
    const ClientSettings & settings,
    const OperationEvents & events,
    const Request & request,
    const std::string & request_params,
    Call && call)
{
    Result<Response> result;
    const UInt64 attempts = maxAttempts(settings);

    for (UInt64 attempt = 1; attempt <= attempts; ++attempt)
    {
        auto context = client.makeContext(result.status, request_params);
        if (!result.ok())
            return result;

        throttleRequest(settings, events);
        recordAttemptStart(events, settings.for_disk);

        Stopwatch watch;
        result.status = fromGrpcStatus(call(*context, request, result.response));
        recordAttemptTime(events, settings.for_disk, watch.elapsedMicroseconds());

        if (result.ok())
            return result;

        recordFailure(events, settings.for_disk, result.status.code);
        if (!shouldRetry(result.status, attempt, attempts))
            return result;
    }

    return result;
}

template <typename Response, typename Request, typename Call>
Result<Response> executeWriteUnaryRequest(
    const Client & client,
    const ClientSettings & settings,
    const Request & request,
    const std::string & request_params,
    Call && call)
{
    Result<Response> result;
    const UInt64 attempts = maxAttempts(settings);

    for (UInt64 attempt = 1; attempt <= attempts; ++attempt)
    {
        auto context = client.makeContext(result.status, request_params);
        if (!result.ok())
            return result;

        settings.request_throttler.throttleHTTPPut();
        result.status = fromGrpcStatus(call(*context, request, result.response));
        if (result.ok())
            return result;

        if (!shouldRetry(result.status, attempt, attempts))
            return result;
    }

    return result;
}

template <typename Stream>
class AccountingReader final : public grpc::ClientReaderInterface<Stream>
{
public:
    AccountingReader(std::unique_ptr<grpc::ClientReaderInterface<Stream>> stream_, OperationEvents events_, bool for_disk_)
        : stream(std::move(stream_))
        , events(events_)
        , for_disk(for_disk_)
    {
    }

    void WaitForInitialMetadata() override { stream->WaitForInitialMetadata(); }
    bool NextMessageSize(uint32_t * size) override { return stream->NextMessageSize(size); }
    bool Read(Stream * message) override { return stream->Read(message); }

    grpc::Status Finish() override
    {
        auto status = stream->Finish();
        if (!finish_accounted && !status.ok())
        {
            finish_accounted = true;
            recordFailure(events, for_disk, fromGrpcStatus(status).code);
        }
        return status;
    }

private:
    std::unique_ptr<grpc::ClientReaderInterface<Stream>> stream;
    OperationEvents events;
    bool for_disk;
    bool finish_accounted = false;
};

template <typename Stream>
class AccountingWriter final : public grpc::ClientWriterInterface<Stream>
{
public:
    AccountingWriter(std::unique_ptr<grpc::ClientWriterInterface<Stream>> stream_, OperationEvents events_, bool for_disk_)
        : stream(std::move(stream_))
        , events(events_)
        , for_disk(for_disk_)
    {
    }

    void WaitForInitialMetadata() { stream->WaitForInitialMetadata(); }
    bool Write(const Stream & message, grpc::WriteOptions options) override { return stream->Write(message, options); }
    bool WritesDone() override { return stream->WritesDone(); }

    grpc::Status Finish() override
    {
        auto status = stream->Finish();
        if (!finish_accounted && !status.ok())
        {
            finish_accounted = true;
            recordFailure(events, for_disk, fromGrpcStatus(status).code);
        }
        return status;
    }

private:
    std::unique_ptr<grpc::ClientWriterInterface<Stream>> stream;
    OperationEvents events;
    bool for_disk;
    bool finish_accounted = false;
};

class GeneratedStub final : public IStub
{
public:
    explicit GeneratedStub(std::unique_ptr<google::storage::v2::Storage::StubInterface> stub_)
        : stub(std::move(stub_))
    {
    }

    grpc::Status getObject(
        grpc::ClientContext & context,
        const google::storage::v2::GetObjectRequest & request,
        google::storage::v2::Object & response) override
    {
        return stub->GetObject(&context, request, &response);
    }

    grpc::Status listObjects(
        grpc::ClientContext & context,
        const google::storage::v2::ListObjectsRequest & request,
        google::storage::v2::ListObjectsResponse & response) override
    {
        return stub->ListObjects(&context, request, &response);
    }

    grpc::Status composeObject(
        grpc::ClientContext & context,
        const google::storage::v2::ComposeObjectRequest & request,
        google::storage::v2::Object & response) override
    {
        return stub->ComposeObject(&context, request, &response);
    }

    grpc::Status rewriteObject(
        grpc::ClientContext & context,
        const google::storage::v2::RewriteObjectRequest & request,
        google::storage::v2::RewriteResponse & response) override
    {
        return stub->RewriteObject(&context, request, &response);
    }

    grpc::Status deleteObject(
        grpc::ClientContext & context,
        const google::storage::v2::DeleteObjectRequest & request,
        google::protobuf::Empty & response) override
    {
        return stub->DeleteObject(&context, request, &response);
    }

    std::unique_ptr<grpc::ClientReaderInterface<google::storage::v2::ReadObjectResponse>>
    readObject(grpc::ClientContext & context, const google::storage::v2::ReadObjectRequest & request) override
    {
        return stub->ReadObject(&context, request);
    }

    std::unique_ptr<grpc::ClientWriterInterface<google::storage::v2::WriteObjectRequest>>
    writeObject(grpc::ClientContext & context, google::storage::v2::WriteObjectResponse & response) override
    {
        return stub->WriteObject(&context, &response);
    }

private:
    std::unique_ptr<google::storage::v2::Storage::StubInterface> stub;
};

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

std::string bucketRoutingParameter(const std::string & bucket)
{
    return "bucket=" + google::cloud::internal::UrlEncode(bucket);
}

std::string rewriteObjectRoutingParameter(const google::storage::v2::RewriteObjectRequest & request)
{
    std::vector<std::string> parameters;
    if (!request.source_bucket().empty())
        parameters.push_back("source_bucket=" + google::cloud::internal::UrlEncode(request.source_bucket()));
    if (!request.destination_bucket().empty())
        parameters.push_back("bucket=" + google::cloud::internal::UrlEncode(request.destination_bucket()));

    std::string result;
    for (const auto & parameter : parameters)
    {
        if (!result.empty())
            result += "&";
        result += parameter;
    }
    return result;
}

grpc::Status nextStatus(std::vector<grpc::Status> & statuses, const grpc::Status & fallback)
{
    if (statuses.empty())
        return fallback;

    auto status = statuses.front();
    statuses.erase(statuses.begin());
    return status;
}

}

Client::Client(
    ClientSettings settings_, std::shared_ptr<IStub> stub_, std::shared_ptr<google::cloud::internal::GrpcAuthenticationStrategy> auth_)
    : settings(std::move(settings_))
    , stub(std::move(stub_))
    , auth(std::move(auth_))
{
    configureRequestThrottlerEvents(settings);
}

std::unique_ptr<grpc::ClientContext> Client::makeContext(Status & status, const std::string & request_params) const
{
    auto context = std::make_unique<grpc::ClientContext>();
    context->set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(settings.request_timeout_ms));

    if (!request_params.empty())
        context->AddMetadata("x-goog-request-params", request_params);

    if (!settings.user_project.empty())
        context->AddMetadata("x-goog-user-project", settings.user_project);

    status = {};
    if (auth && auth->RequiresConfigureContext())
        status = fromCloudStatus(auth->ConfigureContext(*context));

    return context;
}

Result<google::storage::v2::Object> Client::getObject(const google::storage::v2::GetObjectRequest & request) const
{
    return executeUnaryRequest<google::storage::v2::Object>(
        *this,
        settings,
        get_object_events,
        request,
        bucketRoutingParameter(request.bucket()),
        [this](grpc::ClientContext & context, const auto & request_, auto & response)
        { return stub->getObject(context, request_, response); });
}

Result<google::storage::v2::ListObjectsResponse> Client::listObjects(const google::storage::v2::ListObjectsRequest & request) const
{
    return executeUnaryRequest<google::storage::v2::ListObjectsResponse>(
        *this,
        settings,
        list_objects_events,
        request,
        bucketRoutingParameter(request.parent()),
        [this](grpc::ClientContext & context, const auto & request_, auto & response)
        { return stub->listObjects(context, request_, response); });
}

Status Client::deleteObject(const google::storage::v2::DeleteObjectRequest & request) const
{
    auto result = executeUnaryRequest<google::protobuf::Empty>(
        *this,
        settings,
        delete_object_events,
        request,
        bucketRoutingParameter(request.bucket()),
        [this](grpc::ClientContext & context, const auto & request_, auto & response_)
        { return stub->deleteObject(context, request_, response_); });
    return result.status;
}

Result<google::storage::v2::Object> Client::composeObject(const google::storage::v2::ComposeObjectRequest & request) const
{
    return executeWriteUnaryRequest<google::storage::v2::Object>(
        *this,
        settings,
        request,
        bucketRoutingParameter(request.destination().bucket()),
        [this](grpc::ClientContext & context, const auto & request_, auto & response)
        { return stub->composeObject(context, request_, response); });
}

Result<google::storage::v2::RewriteResponse> Client::rewriteObject(const google::storage::v2::RewriteObjectRequest & request) const
{
    return executeWriteUnaryRequest<google::storage::v2::RewriteResponse>(
        *this,
        settings,
        request,
        rewriteObjectRoutingParameter(request),
        [this](grpc::ClientContext & context, const auto & request_, auto & response)
        { return stub->rewriteObject(context, request_, response); });
}

StreamResult<grpc::ClientReaderInterface<google::storage::v2::ReadObjectResponse>>
Client::readObject(const google::storage::v2::ReadObjectRequest & request) const
{
    StreamResult<grpc::ClientReaderInterface<google::storage::v2::ReadObjectResponse>> result;
    const UInt64 attempts = maxAttempts(settings);

    for (UInt64 attempt = 1; attempt <= attempts; ++attempt)
    {
        result.context = makeContext(result.status, bucketRoutingParameter(request.bucket()));
        if (!result.status.ok())
            return result;

        throttleRequest(settings, read_object_events);
        recordAttemptStart(read_object_events, settings.for_disk);

        Stopwatch watch;
        auto stream = stub->readObject(*result.context, request);
        recordAttemptTime(read_object_events, settings.for_disk, watch.elapsedMicroseconds());

        if (stream)
        {
            result.stream = std::make_unique<AccountingReader<google::storage::v2::ReadObjectResponse>>(
                std::move(stream), read_object_events, settings.for_disk);
            result.status = {};
            return result;
        }

        result.status = makeStatus(StatusCode::Unavailable, "GCS gRPC ReadObject did not create a stream");
        recordFailure(read_object_events, settings.for_disk, result.status.code);
        if (!read_object_events.retry_stream_creation || !shouldRetry(result.status, attempt, attempts))
            return result;
    }

    return result;
}

StreamResult<grpc::ClientWriterInterface<google::storage::v2::WriteObjectRequest>>
Client::writeObject(google::storage::v2::WriteObjectResponse & response, const std::string & bucket) const
{
    StreamResult<grpc::ClientWriterInterface<google::storage::v2::WriteObjectRequest>> result;
    const UInt64 attempts = maxAttempts(settings);

    for (UInt64 attempt = 1; attempt <= attempts; ++attempt)
    {
        result.context = makeContext(result.status, bucketRoutingParameter(bucket));
        if (!result.status.ok())
            return result;

        throttleRequest(settings, write_object_events);
        recordAttemptStart(write_object_events, settings.for_disk);

        Stopwatch watch;
        auto stream = stub->writeObject(*result.context, response);
        recordAttemptTime(write_object_events, settings.for_disk, watch.elapsedMicroseconds());

        if (stream)
        {
            result.stream = std::make_unique<AccountingWriter<google::storage::v2::WriteObjectRequest>>(
                std::move(stream), write_object_events, settings.for_disk);
            result.status = {};
            return result;
        }

        result.status = makeStatus(StatusCode::Unavailable, "GCS gRPC WriteObject did not create a stream");
        recordFailure(write_object_events, settings.for_disk, result.status.code);
        if (!write_object_events.retry_stream_creation || !shouldRetry(result.status, attempt, attempts))
            return result;
    }

    return result;
}

std::shared_ptr<Client> createClient(const ClientSettings & settings)
{
    assertGrpcAvailable();

    grpc::ChannelArguments channel_arguments;
    google::cloud::CompletionQueue completion_queue;
    auto auth = google::cloud::internal::CreateAuthenticationStrategy(*makeCredentials(settings), completion_queue);
    auto channel = auth->CreateChannel(settings.endpoint, channel_arguments);
    auto stub = std::make_shared<GeneratedStub>(google::storage::v2::Storage::NewStub(channel));
    return std::make_shared<Client>(settings, std::move(stub), std::move(auth));
}

FakeReadStream::FakeReadStream(std::vector<google::storage::v2::ReadObjectResponse> responses_, grpc::Status finish_status_)
    : responses(std::move(responses_))
    , finish_status(std::move(finish_status_))
{
}

bool FakeReadStream::NextMessageSize(uint32_t * size)
{
    if (next_response >= responses.size())
    {
        *size = 0;
        return false;
    }

    const auto response_size = responses[next_response].ByteSizeLong();
    *size = response_size > std::numeric_limits<uint32_t>::max() ? std::numeric_limits<uint32_t>::max()
                                                                 : static_cast<uint32_t>(response_size);
    return true;
}

bool FakeReadStream::Read(google::storage::v2::ReadObjectResponse * message)
{
    if (next_response >= responses.size())
        return false;

    *message = responses[next_response++];
    return true;
}

grpc::Status FakeReadStream::Finish()
{
    return finish_status;
}

FakeWriteStream::FakeWriteStream(
    google::storage::v2::WriteObjectResponse * response_out_,
    google::storage::v2::WriteObjectResponse response_,
    grpc::Status finish_status_,
    FinishCallback finish_callback_,
    bool write_returns_false_,
    bool writes_done_returns_false_,
    std::atomic_int * finish_calls_)
    : response_out(response_out_)
    , response(std::move(response_))
    , finish_status(std::move(finish_status_))
    , finish_callback(std::move(finish_callback_))
    , write_returns_false(write_returns_false_)
    , writes_done_returns_false(writes_done_returns_false_)
    , finish_calls(finish_calls_)
{
}

bool FakeWriteStream::Write(const google::storage::v2::WriteObjectRequest & message, grpc::WriteOptions)
{
    if (writes_done || write_returns_false)
        return false;

    writes.push_back(message);
    return true;
}

bool FakeWriteStream::WritesDone()
{
    writes_done = true;
    return !writes_done_returns_false;
}

grpc::Status FakeWriteStream::Finish()
{
    if (finish_calls)
        ++*finish_calls;

    if (!finish_status.ok())
        return finish_status;
    if (finish_callback)
    {
        auto callback_status = finish_callback(writes, response);
        if (!callback_status.ok())
            return callback_status;
    }

    if (response_out)
        *response_out = response;

    return finish_status;
}

namespace
{

std::string fakeObjectKey(const std::string & bucket, const std::string & object)
{
    return bucket + "\n" + object;
}

std::string cordToString(const absl::Cord & cord)
{
    std::string result;
    absl::CopyCordToString(cord, &result);
    return result;
}

}

grpc::Status FakeStub::getObject(
    grpc::ClientContext & context, const google::storage::v2::GetObjectRequest & request, google::storage::v2::Object & response)
{
    last_deadline = context.deadline();
    last_metadata = grpc::testing::ClientContextTestPeer(&context).GetSendInitialMetadata();
    get_object_requests.push_back(request);

    auto status = nextStatus(get_object_statuses, get_object_status);
    if (!status.ok())
        return status;

    if (use_object_map)
    {
        auto it = objects.find(fakeObjectKey(request.bucket(), request.object()));
        if (it == objects.end())
            return grpc::Status(grpc::StatusCode::NOT_FOUND, "fake object not found");
        response = it->second.metadata;
        return grpc::Status::OK;
    }

    response = get_object_response;
    return status;
}

grpc::Status FakeStub::listObjects(
    grpc::ClientContext & context,
    const google::storage::v2::ListObjectsRequest & request,
    google::storage::v2::ListObjectsResponse & response)
{
    last_deadline = context.deadline();
    last_metadata = grpc::testing::ClientContextTestPeer(&context).GetSendInitialMetadata();
    list_objects_requests.push_back(request);

    auto status = nextStatus(list_objects_statuses, list_objects_status);
    if (!status.ok())
        return status;

    if (use_object_map)
    {
        std::vector<const google::storage::v2::Object *> matched;
        for (const auto & [key, object] : objects)
        {
            (void)key;
            if (object.metadata.bucket() != request.parent())
                continue;
            if (!request.prefix().empty() && !object.metadata.name().starts_with(request.prefix()))
                continue;
            if (!request.lexicographic_start().empty() && object.metadata.name() < request.lexicographic_start())
                continue;
            matched.push_back(&object.metadata);
        }

        const size_t start = request.page_token().empty() ? 0 : std::stoull(request.page_token());
        const size_t limit = request.page_size() > 0 ? static_cast<size_t>(request.page_size()) : std::numeric_limits<size_t>::max();
        for (size_t i = start; i < matched.size() && static_cast<size_t>(response.objects_size()) < limit; ++i)
            *response.add_objects() = *matched[i];

        const size_t next = start + static_cast<size_t>(response.objects_size());
        if (next < matched.size())
            response.set_next_page_token(std::to_string(next));

        return grpc::Status::OK;
    }

    response = list_objects_response;
    return status;
}

grpc::Status
FakeStub::deleteObject(grpc::ClientContext & context, const google::storage::v2::DeleteObjectRequest & request, google::protobuf::Empty &)
{
    last_deadline = context.deadline();
    last_metadata = grpc::testing::ClientContextTestPeer(&context).GetSendInitialMetadata();
    delete_object_requests.push_back(request);

    auto status = nextStatus(delete_object_statuses, delete_object_status);
    if (!status.ok())
        return status;

    if (use_object_map)
    {
        auto it = objects.find(fakeObjectKey(request.bucket(), request.object()));
        if (it == objects.end())
            return grpc::Status(grpc::StatusCode::NOT_FOUND, "fake object not found");
        objects.erase(it);
    }

    return grpc::Status::OK;
}

grpc::Status FakeStub::composeObject(
    grpc::ClientContext & context, const google::storage::v2::ComposeObjectRequest & request, google::storage::v2::Object & response)
{
    last_deadline = context.deadline();
    last_metadata = grpc::testing::ClientContextTestPeer(&context).GetSendInitialMetadata();
    compose_object_requests.push_back(request);

    if (!compose_object_status.ok())
        return compose_object_status;

    if (use_object_map)
    {
        std::string data;
        for (const auto & source : request.source_objects())
        {
            auto it = objects.find(fakeObjectKey(request.destination().bucket(), source.name()));
            if (it == objects.end())
                return grpc::Status(grpc::StatusCode::NOT_FOUND, "fake compose source object not found");
            data += it->second.data;
        }

        const auto destination_key = fakeObjectKey(request.destination().bucket(), request.destination().name());
        if (request.has_if_generation_match() && request.if_generation_match() == 0 && objects.contains(destination_key))
            return grpc::Status(grpc::StatusCode::ALREADY_EXISTS, "fake compose destination already exists");

        FakeObject object;
        object.data = std::move(data);
        object.metadata = request.destination();
        object.metadata.set_size(static_cast<int64_t>(object.data.size()));
        objects[destination_key] = object;
        response = object.metadata;
        return grpc::Status::OK;
    }

    response = compose_object_response;
    return grpc::Status::OK;
}

grpc::Status FakeStub::rewriteObject(
    grpc::ClientContext & context, const google::storage::v2::RewriteObjectRequest & request, google::storage::v2::RewriteResponse & response)
{
    last_deadline = context.deadline();
    last_metadata = grpc::testing::ClientContextTestPeer(&context).GetSendInitialMetadata();
    rewrite_object_requests.push_back(request);

    if (!rewrite_object_status.ok())
        return rewrite_object_status;

    if (!rewrite_object_responses.empty())
    {
        response = rewrite_object_responses.front();
        rewrite_object_responses.erase(rewrite_object_responses.begin());
        return grpc::Status::OK;
    }

    if (use_object_map)
    {
        auto it = objects.find(fakeObjectKey(request.source_bucket(), request.source_object()));
        if (it == objects.end())
            return grpc::Status(grpc::StatusCode::NOT_FOUND, "fake rewrite source object not found");

        const auto destination_key = fakeObjectKey(request.destination_bucket(), request.destination_name());
        if (request.has_if_generation_match() && request.if_generation_match() == 0 && objects.contains(destination_key))
            return grpc::Status(grpc::StatusCode::ALREADY_EXISTS, "fake rewrite destination already exists");

        FakeObject object;
        object.data = it->second.data;
        object.metadata = request.has_destination() ? request.destination() : it->second.metadata;
        object.metadata.set_bucket(request.destination_bucket());
        object.metadata.set_name(request.destination_name());
        object.metadata.set_size(static_cast<int64_t>(object.data.size()));
        objects[destination_key] = object;

        response.set_done(true);
        response.set_total_bytes_rewritten(static_cast<int64_t>(object.data.size()));
        response.set_object_size(static_cast<int64_t>(object.data.size()));
        *response.mutable_resource() = object.metadata;
        return grpc::Status::OK;
    }

    response = rewrite_object_response;
    return grpc::Status::OK;
}

std::unique_ptr<grpc::ClientReaderInterface<google::storage::v2::ReadObjectResponse>>
FakeStub::readObject(grpc::ClientContext & context, const google::storage::v2::ReadObjectRequest & request)
{
    last_deadline = context.deadline();
    last_metadata = grpc::testing::ClientContextTestPeer(&context).GetSendInitialMetadata();
    read_object_requests.push_back(request);

    if (read_object_null_streams > 0)
    {
        --read_object_null_streams;
        return nullptr;
    }

    if (use_object_map)
    {
        auto it = objects.find(fakeObjectKey(request.bucket(), request.object()));
        if (it == objects.end())
            return std::make_unique<FakeReadStream>(
                std::vector<google::storage::v2::ReadObjectResponse>{}, grpc::Status(grpc::StatusCode::NOT_FOUND, "fake object not found"));

        const auto & data = it->second.data;
        size_t offset = request.read_offset() > 0 ? static_cast<size_t>(request.read_offset()) : 0;
        if (offset > data.size())
            offset = data.size();
        size_t size = data.size() - offset;
        if (request.read_limit() > 0)
            size = std::min(size, static_cast<size_t>(request.read_limit()));

        google::storage::v2::ReadObjectResponse response;
        *response.mutable_metadata() = it->second.metadata;
        response.mutable_checksummed_data()->set_content(std::string_view(data).substr(offset, size));
        return std::make_unique<FakeReadStream>(std::vector<google::storage::v2::ReadObjectResponse>{response}, grpc::Status::OK);
    }

    return std::make_unique<FakeReadStream>(read_object_responses, read_object_finish_status);
}

std::unique_ptr<grpc::ClientWriterInterface<google::storage::v2::WriteObjectRequest>>
FakeStub::writeObject(grpc::ClientContext & context, google::storage::v2::WriteObjectResponse & response)
{
    google::storage::v2::WriteObjectResponse response_template;
    grpc::Status finish_status;
    bool write_returns_false = false;
    bool writes_done_returns_false = false;

    {
        std::lock_guard lock(mutex);
        last_deadline = context.deadline();
        last_metadata = grpc::testing::ClientContextTestPeer(&context).GetSendInitialMetadata();
        ++write_object_stream_creations;
        response = write_object_response;

        if (write_object_null_streams > 0)
        {
            --write_object_null_streams;
            return nullptr;
        }

        response_template = write_object_response;
        finish_status = write_object_finish_status;
        write_returns_false = write_object_write_returns_false;
        writes_done_returns_false = write_object_writes_done_returns_false;
    }

    auto finish_callback =
        [this](
            const std::vector<google::storage::v2::WriteObjectRequest> & writes, google::storage::v2::WriteObjectResponse & write_response)
    {
        std::lock_guard lock(mutex);
        write_object_requests.insert(write_object_requests.end(), writes.begin(), writes.end());

        if (!use_object_map)
            return grpc::Status::OK;

        if (writes.empty() || !writes.front().has_write_object_spec())
            return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "missing fake write object spec");

        const auto & resource = writes.front().write_object_spec().resource();
        std::string data;
        for (const auto & write : writes)
        {
            if (write.has_checksummed_data())
                data += cordToString(write.checksummed_data().content());
        }

        const auto object_key = fakeObjectKey(resource.bucket(), resource.name());
        const auto & spec = writes.front().write_object_spec();
        if (spec.has_if_generation_match() && spec.if_generation_match() == 0 && objects.contains(object_key))
            return grpc::Status(grpc::StatusCode::ALREADY_EXISTS, "fake write destination already exists");

        FakeObject object;
        object.data = std::move(data);
        object.metadata = resource;
        object.metadata.set_size(static_cast<int64_t>(object.data.size()));
        objects[object_key] = object;
        *write_response.mutable_resource() = object.metadata;
        return grpc::Status::OK;
    };

    return std::make_unique<FakeWriteStream>(
        &response,
        response_template,
        finish_status,
        std::move(finish_callback),
        write_returns_false,
        writes_done_returns_false,
        &write_object_finish_calls);
}

#endif
}
