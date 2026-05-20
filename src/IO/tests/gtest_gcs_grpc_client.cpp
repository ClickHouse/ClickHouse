#include <IO/GCS/GCSClient.h>
#include <IO/GCS/GCSStatus.h>

#include <Common/Exception.h>
#include <gtest/gtest.h>

namespace DB::ErrorCodes
{
extern const int ACCESS_DENIED;
extern const int BAD_ARGUMENTS;
extern const int FILE_DOESNT_EXIST;
extern const int NETWORK_ERROR;
extern const int NOT_IMPLEMENTED;
extern const int S3_ERROR;
extern const int TIMEOUT_EXCEEDED;
}

using namespace DB;

TEST(GCSGrpcClientFoundation, CredentialMode)
{
    GCS::ClientSettings settings;
    EXPECT_EQ(GCS::CredentialMode::GoogleDefault, GCS::credentialMode(settings));
    EXPECT_STREQ("google_default", GCS::credentialModeName(GCS::credentialMode(settings)));

    settings.service_account_json = "{}";
    EXPECT_EQ(GCS::CredentialMode::ServiceAccountKey, GCS::credentialMode(settings));
    EXPECT_STREQ("service_account_key", GCS::credentialModeName(GCS::credentialMode(settings)));

    settings.use_insecure_credentials_for_tests = true;
    EXPECT_EQ(GCS::CredentialMode::InsecureForTests, GCS::credentialMode(settings));
    EXPECT_STREQ("insecure_for_tests", GCS::credentialModeName(GCS::credentialMode(settings)));
}

TEST(GCSGrpcClientFoundation, StatusMapping)
{
    EXPECT_EQ(0, GCS::errorCodeForStatus(GCS::StatusCode::OK));
    EXPECT_EQ(ErrorCodes::FILE_DOESNT_EXIST, GCS::errorCodeForStatus(GCS::StatusCode::NotFound));
    EXPECT_EQ(ErrorCodes::ACCESS_DENIED, GCS::errorCodeForStatus(GCS::StatusCode::PermissionDenied));
    EXPECT_EQ(ErrorCodes::TIMEOUT_EXCEEDED, GCS::errorCodeForStatus(GCS::StatusCode::DeadlineExceeded));
    EXPECT_EQ(ErrorCodes::NETWORK_ERROR, GCS::errorCodeForStatus(GCS::StatusCode::ResourceExhausted));
    EXPECT_EQ(ErrorCodes::NETWORK_ERROR, GCS::errorCodeForStatus(GCS::StatusCode::Unavailable));
    EXPECT_EQ(ErrorCodes::BAD_ARGUMENTS, GCS::errorCodeForStatus(GCS::StatusCode::InvalidArgument));
    EXPECT_EQ(ErrorCodes::NOT_IMPLEMENTED, GCS::errorCodeForStatus(GCS::StatusCode::Unsupported));
    EXPECT_EQ(ErrorCodes::S3_ERROR, GCS::errorCodeForStatus(GCS::StatusCode::Unknown));

    EXPECT_TRUE(GCS::isRetryableStatus(GCS::StatusCode::ResourceExhausted));
    EXPECT_TRUE(GCS::isRetryableStatus(GCS::StatusCode::Unavailable));
    EXPECT_TRUE(GCS::isRetryableStatus(GCS::StatusCode::DeadlineExceeded));
    EXPECT_FALSE(GCS::isRetryableStatus(GCS::StatusCode::NotFound));

    EXPECT_TRUE(GCS::isThrottlingStatus(GCS::StatusCode::ResourceExhausted));
    EXPECT_FALSE(GCS::isThrottlingStatus(GCS::StatusCode::Unavailable));
    EXPECT_FALSE(GCS::isThrottlingStatus(GCS::StatusCode::DeadlineExceeded));
    EXPECT_FALSE(GCS::isThrottlingStatus(GCS::StatusCode::NotFound));
}

TEST(GCSGrpcClientFoundation, AvailabilityGuard)
{
#if USE_GOOGLE_CLOUD
    EXPECT_TRUE(GCS::isGrpcAvailable());
    EXPECT_NO_THROW(GCS::assertGrpcAvailable());
#else
    EXPECT_FALSE(GCS::isGrpcAvailable());
    EXPECT_THROW(GCS::assertGrpcAvailable(), DB::Exception);
#endif
}

#if USE_GOOGLE_CLOUD
TEST(GCSGrpcClientFoundation, CloudStatusMapping)
{
    EXPECT_TRUE(GCS::fromCloudStatus(google::cloud::Status()).ok());
    EXPECT_EQ(
        GCS::StatusCode::NotFound,
        GCS::fromCloudStatus(google::cloud::Status(google::cloud::StatusCode::kNotFound, "missing")).code);
    EXPECT_EQ(
        GCS::StatusCode::PermissionDenied,
        GCS::fromCloudStatus(google::cloud::Status(google::cloud::StatusCode::kPermissionDenied, "denied")).code);
    EXPECT_EQ(
        GCS::StatusCode::DeadlineExceeded,
        GCS::fromCloudStatus(google::cloud::Status(google::cloud::StatusCode::kDeadlineExceeded, "deadline")).code);
    EXPECT_EQ(
        GCS::StatusCode::ResourceExhausted,
        GCS::fromCloudStatus(google::cloud::Status(google::cloud::StatusCode::kResourceExhausted, "quota")).code);
    EXPECT_EQ(
        GCS::StatusCode::Unavailable,
        GCS::fromCloudStatus(google::cloud::Status(google::cloud::StatusCode::kUnavailable, "unavailable")).code);
    EXPECT_EQ(
        GCS::StatusCode::InvalidArgument,
        GCS::fromCloudStatus(google::cloud::Status(google::cloud::StatusCode::kFailedPrecondition, "failed precondition")).code);
    EXPECT_EQ(
        GCS::StatusCode::Unsupported,
        GCS::fromCloudStatus(google::cloud::Status(google::cloud::StatusCode::kUnimplemented, "unsupported")).code);
}

TEST(GCSGrpcClientFoundation, OptionsMapClientSettings)
{
    GCS::ClientSettings settings;
    settings.endpoint = "localhost:4443";
    settings.request_timeout_ms = 2500;
    settings.service_account_json = R"({"type":"service_account"})";
    settings.user_project = "billing-project";

    auto options = GCS::makeGrpcClientOptions(settings);
    EXPECT_EQ("localhost:4443", options.get<google::cloud::EndpointOption>());
    EXPECT_TRUE(options.has<google::cloud::UnifiedCredentialsOption>());
    EXPECT_EQ("billing-project", options.get<google::cloud::UserProjectOption>());
    EXPECT_TRUE(options.has<google::cloud::storage::RetryPolicyOption>());
    EXPECT_TRUE(options.has<google::cloud::storage::TransferStallTimeoutOption>());
}
#endif
