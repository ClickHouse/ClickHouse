#include "config.h"

#if USE_GOOGLE_CLOUD

#include <Disks/DiskObjectStorage/ObjectStorages/GCS/gcsSettings.h>
#include <gtest/gtest.h>

using namespace DB;

/// The authentication modes of a native GCS backend are mutually exclusive, and their priority order
/// decides both which credentials the client is built with and whether the `google_adc_*`
/// refresh-token triple has to be exchanged for an access token at all.

TEST(GCSCredentialSource, EmptySettingsUseApplicationDefault)
{
    GCSObjectStorageSettings settings;
    EXPECT_EQ(chooseGCSCredentialSource(settings), GCSCredentialSource::ApplicationDefault);
}

TEST(GCSCredentialSource, AnonymousWins)
{
    GCSObjectStorageSettings settings;
    settings.no_sign_request = true;
    settings.service_account_key = "{}";
    settings.service_account_key_file = "/dev/null";
    settings.access_token = "token";
    EXPECT_EQ(chooseGCSCredentialSource(settings), GCSCredentialSource::Anonymous);
}

TEST(GCSCredentialSource, InlineKeyBeatsKeyFileAndToken)
{
    GCSObjectStorageSettings settings;
    settings.service_account_key = "{}";
    settings.service_account_key_file = "/dev/null";
    settings.access_token = "token";
    EXPECT_EQ(chooseGCSCredentialSource(settings), GCSCredentialSource::ServiceAccountKey);
}

TEST(GCSCredentialSource, KeyFileBeatsToken)
{
    GCSObjectStorageSettings settings;
    settings.service_account_key_file = "/dev/null";
    settings.access_token = "token";
    EXPECT_EQ(chooseGCSCredentialSource(settings), GCSCredentialSource::ServiceAccountKeyFile);
}

TEST(GCSCredentialSource, TokenIsTheLastExplicitMode)
{
    GCSObjectStorageSettings settings;
    settings.access_token = "token";
    EXPECT_EQ(chooseGCSCredentialSource(settings), GCSCredentialSource::AccessToken);
}

/// The refresh-token triple is not an authentication mode of its own: it only feeds `access_token`.
/// A configuration that carries it next to a higher-priority mode must not even try to exchange it,
/// so a stale triple cannot break an otherwise valid configuration. `resolveGCSCredentialsToken`
/// tests that by asking this function, so the two can never disagree.
TEST(GCSCredentialSource, RefreshTokenTripleAloneIsNotAMode)
{
    GCSObjectStorageSettings settings;
    settings.google_adc_client_id = "id";
    settings.google_adc_client_secret = "secret";
    settings.google_adc_refresh_token = "refresh";
    EXPECT_EQ(chooseGCSCredentialSource(settings), GCSCredentialSource::ApplicationDefault);

    settings.no_sign_request = true;
    EXPECT_EQ(chooseGCSCredentialSource(settings), GCSCredentialSource::Anonymous);
}

/// The transport knobs are part of the client identity, so a cross-storage server-side copy does not
/// run one storage's request through another storage's client when they differ.
TEST(GCSCredentialSource, TransportKnobsArePartOfClientIdentity)
{
    GCSObjectStorageSettings lhs;
    GCSObjectStorageSettings rhs;
    lhs.no_sign_request = true;
    rhs.no_sign_request = true;
    EXPECT_TRUE(lhs.describesSameClientAs(rhs));

    rhs.headers.emplace_back("x-custom", "value");
    EXPECT_FALSE(lhs.describesSameClientAs(rhs));

    rhs = lhs;
    rhs.connect_timeout_ms = lhs.connect_timeout_ms + 1;
    EXPECT_FALSE(lhs.describesSameClientAs(rhs));

    rhs = lhs;
    rhs.request_timeout_ms = lhs.request_timeout_ms + 1;
    EXPECT_FALSE(lhs.describesSameClientAs(rhs));

    rhs = lhs;
    rhs.max_connections = lhs.max_connections + 1;
    EXPECT_FALSE(lhs.describesSameClientAs(rhs));
}

TEST(GCSCredentialSource, ApplicationDefaultCredentialsAreNotInterchangeable)
{
    GCSObjectStorageSettings lhs;
    GCSObjectStorageSettings rhs;
    EXPECT_FALSE(lhs.describesSameClientAs(rhs));
}

#endif
