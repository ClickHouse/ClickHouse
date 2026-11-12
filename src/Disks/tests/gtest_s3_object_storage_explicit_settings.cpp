#include <gtest/gtest.h>

#include "config.h"

#if USE_AWS_S3

#include <sstream>

#include <Poco/Util/XMLConfiguration.h>

#include <Common/RemoteHostFilter.h>
#include <Common/tests/gtest_global_context.h>
#include <Disks/DiskObjectStorage/ObjectStorages/S3/S3ObjectStorage.h>
#include <IO/S3/Client.h>
#include <IO/S3/PocoHTTPClient.h>
#include <IO/S3/S3Capabilities.h>
#include <IO/S3/URI.h>
#include <IO/S3RequestSettings.h>
#include <IO/S3Settings.h>
#include <Interpreters/Context.h>

namespace DB::S3RequestSetting
{
    extern const S3RequestSettingsString upload_checksum_algorithm;
    extern const S3RequestSettingsString delete_checksum_algorithm;
}

using namespace DB;

namespace
{

constexpr auto endpoint_config = R"(
<clickhouse>
    <s3>
        <test_endpoint>
            <endpoint>http://localhost:11111/test/explicit_settings</endpoint>
            <upload_checksum_algorithm>MD5</upload_checksum_algorithm>
            <delete_checksum_algorithm>MD5</delete_checksum_algorithm>
        </test_endpoint>
    </s3>
</clickhouse>
)";

std::unique_ptr<S3::Client> createClient(const S3::URI & uri)
{
    RemoteHostFilter remote_host_filter;
    S3::PocoHTTPClientConfiguration client_configuration = S3::ClientFactory::instance().createClientConfiguration(
        "us-east-1",
        remote_host_filter,
        /* s3_max_redirects= */ 0,
        S3::PocoHTTPClientConfiguration::RetryStrategy{.max_retries = 0},
        /* s3_slow_all_threads_after_network_error= */ false,
        /* s3_slow_all_threads_after_retryable_error= */ false,
        /* enable_s3_requests_logging= */ false,
        /* for_disk_s3= */ false,
        /* opt_disk_name= */ {},
        /* request_throttler= */ {},
        uri.uri.getScheme());
    client_configuration.endpointOverride = uri.endpoint;

    S3::ClientSettings client_settings{
        .use_virtual_addressing = uri.is_virtual_hosted_style,
        .gcs_issue_compose_request = false,
        .is_s3express_bucket = false,
    };
    return S3::ClientFactory::instance().create(
        client_configuration,
        client_settings,
        "ACCESS_KEY_ID",
        "SECRET_ACCESS_KEY",
        /* server_side_encryption_customer_key_base64= */ "",
        {},
        {},
        S3::CredentialsConfiguration{.use_environment_credentials = false, .use_insecure_imds_request = false});
}

S3Settings applyNewSettings(const std::map<String, String> & explicit_request_settings)
{
    std::istringstream config_stream{endpoint_config}; /// NOLINT(readability-redundant-string-init)
    Poco::AutoPtr<Poco::Util::XMLConfiguration> config(new Poco::Util::XMLConfiguration(config_stream));

    auto context = getContext().context;
    context->setConfig(config);
    /// The endpoint settings are loaded once and cached, so reload them if another test loaded them first.
    context->getStorageS3Settings();
    context->updateStorageConfiguration(*config);

    S3::URI uri("http://localhost:11111/test/explicit_settings/data.csv");
    S3ObjectStorage storage(
        createClient(uri),
        std::make_unique<S3Settings>(),
        uri,
        S3Capabilities{},
        /* key_generator_= */ nullptr,
        "StorageS3",
        /* for_disk_s3_= */ false,
        /* credentials_refresh_callback_= */ S3ObjectStorage::S3CredentialsRefreshCallback{},
        context->shouldRestrictUserQueryS3Credentials());

    IObjectStorage::ApplyNewSettingsOptions options{.allow_client_change = false};
    options.explicit_request_settings = explicit_request_settings;
    storage.applyNewSettings(context->getConfigRef(), "s3.", context, options);

    return storage.getS3Settings();
}

}

/// The checksum algorithms of an endpoint are applied on every settings refresh, so the arguments of the storage
/// definition have to be re-applied on top of them.
TEST(S3ObjectStorage, ExplicitRequestSettingsOverrideEndpoint)
{
    auto settings = applyNewSettings({{"upload_checksum_algorithm", "sha256"}, {"delete_checksum_algorithm", "crc32"}});
    EXPECT_EQ(settings.request_settings[S3RequestSetting::upload_checksum_algorithm].value, "SHA256");
    EXPECT_EQ(settings.request_settings[S3RequestSetting::delete_checksum_algorithm].value, "CRC32");
}

TEST(S3ObjectStorage, EndpointSettingsApplyWithoutExplicitSettings)
{
    auto settings = applyNewSettings({});
    EXPECT_EQ(settings.request_settings[S3RequestSetting::upload_checksum_algorithm].value, "MD5");
    EXPECT_EQ(settings.request_settings[S3RequestSetting::delete_checksum_algorithm].value, "MD5");
}

#endif
