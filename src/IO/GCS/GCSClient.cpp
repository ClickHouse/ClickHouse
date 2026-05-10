#include <IO/GCS/GCSClient.h>

#include <Common/Exception.h>

#include <limits>

#if USE_GOOGLE_CLOUD
#    include <google/cloud/completion_queue.h>
#    include <google/cloud/credentials.h>
#    include <google/cloud/internal/unified_grpc_credentials.h>
#endif

namespace DB::ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
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
        case google::cloud::StatusCode::kUnavailable:
        case google::cloud::StatusCode::kResourceExhausted:
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

    grpc::Status deleteObject(
        grpc::ClientContext & context,
        const google::storage::v2::DeleteObjectRequest & request,
        google::protobuf::Empty & response) override
    {
        return stub->DeleteObject(&context, request, &response);
    }

    std::unique_ptr<grpc::ClientReaderInterface<google::storage::v2::ReadObjectResponse>> readObject(
        grpc::ClientContext & context,
        const google::storage::v2::ReadObjectRequest & request) override
    {
        return stub->ReadObject(&context, request);
    }

    std::unique_ptr<grpc::ClientWriterInterface<google::storage::v2::WriteObjectRequest>> writeObject(
        grpc::ClientContext & context,
        google::storage::v2::WriteObjectResponse & response) override
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

}

Client::Client(
    ClientSettings settings_,
    std::shared_ptr<IStub> stub_,
    std::shared_ptr<google::cloud::internal::GrpcAuthenticationStrategy> auth_)
    : settings(std::move(settings_))
    , stub(std::move(stub_))
    , auth(std::move(auth_))
{
}

std::unique_ptr<grpc::ClientContext> Client::makeContext(Status & status) const
{
    auto context = std::make_unique<grpc::ClientContext>();
    context->set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(settings.request_timeout_ms));

    if (!settings.user_project.empty())
        context->AddMetadata("x-goog-user-project", settings.user_project);

    status = {};
    if (auth && auth->RequiresConfigureContext())
        status = fromCloudStatus(auth->ConfigureContext(*context));

    return context;
}

Result<google::storage::v2::Object> Client::getObject(const google::storage::v2::GetObjectRequest & request) const
{
    Result<google::storage::v2::Object> result;
    auto context = makeContext(result.status);
    if (!result.ok())
        return result;

    result.status = fromGrpcStatus(stub->getObject(*context, request, result.response));
    return result;
}

Result<google::storage::v2::ListObjectsResponse> Client::listObjects(const google::storage::v2::ListObjectsRequest & request) const
{
    Result<google::storage::v2::ListObjectsResponse> result;
    auto context = makeContext(result.status);
    if (!result.ok())
        return result;

    result.status = fromGrpcStatus(stub->listObjects(*context, request, result.response));
    return result;
}

Status Client::deleteObject(const google::storage::v2::DeleteObjectRequest & request) const
{
    Status status;
    auto context = makeContext(status);
    if (!status.ok())
        return status;

    google::protobuf::Empty response;
    return fromGrpcStatus(stub->deleteObject(*context, request, response));
}

StreamResult<grpc::ClientReaderInterface<google::storage::v2::ReadObjectResponse>> Client::readObject(
    const google::storage::v2::ReadObjectRequest & request) const
{
    StreamResult<grpc::ClientReaderInterface<google::storage::v2::ReadObjectResponse>> result;
    result.context = makeContext(result.status);
    if (!result.status.ok())
        return result;

    result.stream = stub->readObject(*result.context, request);
    if (!result.stream)
        result.status = makeStatus(StatusCode::Unknown, "GCS gRPC ReadObject did not create a stream");
    return result;
}

StreamResult<grpc::ClientWriterInterface<google::storage::v2::WriteObjectRequest>> Client::writeObject(
    google::storage::v2::WriteObjectResponse & response) const
{
    StreamResult<grpc::ClientWriterInterface<google::storage::v2::WriteObjectRequest>> result;
    result.context = makeContext(result.status);
    if (!result.status.ok())
        return result;

    result.stream = stub->writeObject(*result.context, response);
    if (!result.stream)
        result.status = makeStatus(StatusCode::Unknown, "GCS gRPC WriteObject did not create a stream");
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
    *size = response_size > std::numeric_limits<uint32_t>::max()
        ? std::numeric_limits<uint32_t>::max()
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

FakeWriteStream::FakeWriteStream(google::storage::v2::WriteObjectResponse response_, grpc::Status finish_status_)
    : response(std::move(response_))
    , finish_status(std::move(finish_status_))
{
}

bool FakeWriteStream::Write(const google::storage::v2::WriteObjectRequest & message, grpc::WriteOptions)
{
    if (writes_done)
        return false;

    writes.push_back(message);
    return true;
}

bool FakeWriteStream::WritesDone()
{
    writes_done = true;
    return true;
}

grpc::Status FakeWriteStream::Finish()
{
    return finish_status;
}

grpc::Status FakeStub::getObject(
    grpc::ClientContext & context,
    const google::storage::v2::GetObjectRequest & request,
    google::storage::v2::Object & response)
{
    last_deadline = context.deadline();
    last_metadata = context.GetServerInitialMetadata();
    get_object_requests.push_back(request);
    response = get_object_response;
    return get_object_status;
}

grpc::Status FakeStub::listObjects(
    grpc::ClientContext & context,
    const google::storage::v2::ListObjectsRequest & request,
    google::storage::v2::ListObjectsResponse & response)
{
    last_deadline = context.deadline();
    last_metadata = context.GetServerInitialMetadata();
    list_objects_requests.push_back(request);
    response = list_objects_response;
    return list_objects_status;
}

grpc::Status FakeStub::deleteObject(
    grpc::ClientContext & context,
    const google::storage::v2::DeleteObjectRequest & request,
    google::protobuf::Empty &)
{
    last_deadline = context.deadline();
    last_metadata = context.GetServerInitialMetadata();
    delete_object_requests.push_back(request);
    return delete_object_status;
}

std::unique_ptr<grpc::ClientReaderInterface<google::storage::v2::ReadObjectResponse>> FakeStub::readObject(
    grpc::ClientContext & context,
    const google::storage::v2::ReadObjectRequest & request)
{
    last_deadline = context.deadline();
    last_metadata = context.GetServerInitialMetadata();
    read_object_requests.push_back(request);
    return std::make_unique<FakeReadStream>(read_object_responses, read_object_finish_status);
}

std::unique_ptr<grpc::ClientWriterInterface<google::storage::v2::WriteObjectRequest>> FakeStub::writeObject(
    grpc::ClientContext & context,
    google::storage::v2::WriteObjectResponse & response)
{
    last_deadline = context.deadline();
    last_metadata = context.GetServerInitialMetadata();
    response = write_object_response;
    return std::make_unique<FakeWriteStream>(write_object_response, write_object_finish_status);
}

#endif

}
