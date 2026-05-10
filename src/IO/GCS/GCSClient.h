#pragma once

#include <IO/GCS/GCSStatus.h>

#include <base/types.h>
#include <config.h>

#include <chrono>
#include <map>
#include <memory>
#include <vector>

#if USE_GOOGLE_CLOUD
#    include <google/cloud/internal/unified_grpc_credentials.h>
#    include <google/protobuf/empty.pb.h>
#    include <google/storage/v2/storage.grpc.pb.h>
#    include <grpcpp/grpcpp.h>
#endif

namespace DB::GCS
{

struct ClientSettings
{
    String endpoint = "storage.googleapis.com";
    UInt64 request_timeout_ms = 30000;
    String service_account_json;
    String user_project;
    bool use_insecure_credentials_for_tests = false;
};

enum class CredentialMode
{
    GoogleDefault,
    ServiceAccountKey,
    InsecureForTests,
};

CredentialMode credentialMode(const ClientSettings & settings);
const char * credentialModeName(CredentialMode mode);
bool isGrpcAvailable();
void assertGrpcAvailable();

#if USE_GOOGLE_CLOUD

template <typename Response>
struct Result
{
    Status status;
    Response response;

    bool ok() const { return status.ok(); }
};

template <typename Stream>
struct StreamResult
{
    Status status;
    std::unique_ptr<grpc::ClientContext> context;
    std::unique_ptr<Stream> stream;

    bool ok() const { return status.ok() && stream != nullptr; }
};

class IStub
{
public:
    virtual ~IStub() = default;

    virtual grpc::Status getObject(
        grpc::ClientContext & context,
        const google::storage::v2::GetObjectRequest & request,
        google::storage::v2::Object & response) = 0;

    virtual grpc::Status listObjects(
        grpc::ClientContext & context,
        const google::storage::v2::ListObjectsRequest & request,
        google::storage::v2::ListObjectsResponse & response) = 0;

    virtual grpc::Status deleteObject(
        grpc::ClientContext & context,
        const google::storage::v2::DeleteObjectRequest & request,
        google::protobuf::Empty & response) = 0;

    virtual std::unique_ptr<grpc::ClientReaderInterface<google::storage::v2::ReadObjectResponse>> readObject(
        grpc::ClientContext & context,
        const google::storage::v2::ReadObjectRequest & request) = 0;

    virtual std::unique_ptr<grpc::ClientWriterInterface<google::storage::v2::WriteObjectRequest>> writeObject(
        grpc::ClientContext & context,
        google::storage::v2::WriteObjectResponse & response) = 0;
};

class Client
{
public:
    Client(
        ClientSettings settings_,
        std::shared_ptr<IStub> stub_,
        std::shared_ptr<google::cloud::internal::GrpcAuthenticationStrategy> auth_ = nullptr);

    Result<google::storage::v2::Object> getObject(const google::storage::v2::GetObjectRequest & request) const;
    Result<google::storage::v2::ListObjectsResponse> listObjects(const google::storage::v2::ListObjectsRequest & request) const;
    Status deleteObject(const google::storage::v2::DeleteObjectRequest & request) const;

    StreamResult<grpc::ClientReaderInterface<google::storage::v2::ReadObjectResponse>> readObject(
        const google::storage::v2::ReadObjectRequest & request) const;

    StreamResult<grpc::ClientWriterInterface<google::storage::v2::WriteObjectRequest>> writeObject(
        google::storage::v2::WriteObjectResponse & response) const;

    const ClientSettings & getSettings() const { return settings; }

private:
    std::unique_ptr<grpc::ClientContext> makeContext(Status & status) const;

    ClientSettings settings;
    std::shared_ptr<IStub> stub;
    std::shared_ptr<google::cloud::internal::GrpcAuthenticationStrategy> auth;
};

std::shared_ptr<Client> createClient(const ClientSettings & settings);

class FakeReadStream final : public grpc::ClientReaderInterface<google::storage::v2::ReadObjectResponse>
{
public:
    FakeReadStream(std::vector<google::storage::v2::ReadObjectResponse> responses_, grpc::Status finish_status_);

    void WaitForInitialMetadata() override {}
    bool NextMessageSize(uint32_t * size) override;
    bool Read(google::storage::v2::ReadObjectResponse * message) override;
    grpc::Status Finish() override;

private:
    std::vector<google::storage::v2::ReadObjectResponse> responses;
    grpc::Status finish_status;
    size_t next_response = 0;
};

class FakeWriteStream final : public grpc::ClientWriterInterface<google::storage::v2::WriteObjectRequest>
{
public:
    FakeWriteStream(google::storage::v2::WriteObjectResponse response_, grpc::Status finish_status_);

    void WaitForInitialMetadata() {}
    bool Write(const google::storage::v2::WriteObjectRequest & message, grpc::WriteOptions options) override;
    bool WritesDone() override;
    grpc::Status Finish() override;

    const std::vector<google::storage::v2::WriteObjectRequest> & getWrites() const { return writes; }

private:
    google::storage::v2::WriteObjectResponse response;
    grpc::Status finish_status;
    std::vector<google::storage::v2::WriteObjectRequest> writes;
    bool writes_done = false;
};

class FakeStub final : public IStub
{
public:
    grpc::Status get_object_status;
    google::storage::v2::Object get_object_response;
    grpc::Status list_objects_status;
    google::storage::v2::ListObjectsResponse list_objects_response;
    grpc::Status delete_object_status;
    std::vector<google::storage::v2::ReadObjectResponse> read_object_responses;
    grpc::Status read_object_finish_status;
    google::storage::v2::WriteObjectResponse write_object_response;
    grpc::Status write_object_finish_status;

    std::vector<google::storage::v2::GetObjectRequest> get_object_requests;
    std::vector<google::storage::v2::ListObjectsRequest> list_objects_requests;
    std::vector<google::storage::v2::DeleteObjectRequest> delete_object_requests;
    std::vector<google::storage::v2::ReadObjectRequest> read_object_requests;

    grpc::Status getObject(
        grpc::ClientContext & context,
        const google::storage::v2::GetObjectRequest & request,
        google::storage::v2::Object & response) override;

    grpc::Status listObjects(
        grpc::ClientContext & context,
        const google::storage::v2::ListObjectsRequest & request,
        google::storage::v2::ListObjectsResponse & response) override;

    grpc::Status deleteObject(
        grpc::ClientContext & context,
        const google::storage::v2::DeleteObjectRequest & request,
        google::protobuf::Empty & response) override;

    std::unique_ptr<grpc::ClientReaderInterface<google::storage::v2::ReadObjectResponse>> readObject(
        grpc::ClientContext & context,
        const google::storage::v2::ReadObjectRequest & request) override;

    std::unique_ptr<grpc::ClientWriterInterface<google::storage::v2::WriteObjectRequest>> writeObject(
        grpc::ClientContext & context,
        google::storage::v2::WriteObjectResponse & response) override;

    std::chrono::system_clock::time_point last_deadline;
    std::multimap<grpc::string_ref, grpc::string_ref> last_metadata;
};

#endif

}
