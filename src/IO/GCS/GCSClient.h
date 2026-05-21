#pragma once

#include <IO/GCS/GCSStatus.h>
#include <IO/HTTPRequestThrottler.h>

#include <config.h>
#include <base/types.h>

#include <map>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#if USE_GOOGLE_CLOUD
#    include <google/cloud/storage/client.h>
#endif

namespace DB::GCS
{

struct ClientSettings
{
    String endpoint = "storage.googleapis.com";
    UInt64 request_timeout_ms = 30000;
    UInt64 max_retry_attempts = 1;
    String service_account_json;
    String user_project;
    bool use_insecure_credentials_for_tests = false;
    bool for_disk = false;
    HTTPRequestThrottler request_throttler;
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

Status fromCloudStatus(const google::cloud::Status & status);
google::cloud::Options makeGrpcClientOptions(const ClientSettings & settings);

struct ReadResult
{
    Status status;
    google::cloud::storage::ObjectReadStream stream;

    bool ok() const { return status.ok(); }
};

class Client
{
public:
    Client(ClientSettings settings_, google::cloud::Options options_, google::cloud::storage::Client client_);

    const ClientSettings & getSettings() const { return settings; }
    const google::cloud::Options & getOptions() const { return options; }
    google::cloud::storage::Client & getStorageClient() { return client; }
    const google::cloud::storage::Client & getStorageClient() const { return client; }

    ReadResult readObject(
        const std::string & bucket, const std::string & object, size_t offset, std::optional<size_t> limit);
    Result<google::cloud::storage::ObjectMetadata> insertObject(
        const std::string & bucket,
        const std::string & object,
        std::string_view payload,
        const std::map<std::string, std::string> & metadata,
        bool if_generation_match_zero);
    Result<google::cloud::storage::ObjectMetadata> composeObject(
        const std::string & bucket,
        const std::vector<std::string> & sources,
        const std::string & destination,
        const std::map<std::string, std::string> & metadata,
        bool if_generation_match_zero);
    Result<google::cloud::storage::ObjectMetadata> rewriteObject(
        const std::string & source_bucket,
        const std::string & source_object,
        const std::string & destination_bucket,
        const std::string & destination_object,
        const std::map<std::string, std::string> & metadata,
        bool if_generation_match_zero);
    Result<google::cloud::storage::ObjectMetadata> getObjectMetadata(const std::string & bucket, const std::string & object);
    Result<std::vector<google::cloud::storage::ObjectMetadata>> listObjects(
        const std::string & bucket, const std::string & prefix, size_t max_keys, const std::optional<std::string> & start_after);
    Status deleteObject(const std::string & bucket, const std::string & object);
    void recordReadObjectFailure(const Status & status) const;
    void recordWriteObjectFailure(const Status & status) const;

private:
    ClientSettings settings;
    google::cloud::Options options;
    google::cloud::storage::Client client;
};

std::shared_ptr<Client> createClient(const ClientSettings & settings);

#endif

}
