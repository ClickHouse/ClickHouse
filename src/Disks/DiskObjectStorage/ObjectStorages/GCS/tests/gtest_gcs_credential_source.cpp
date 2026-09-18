#include "config.h"

#if USE_GOOGLE_CLOUD

#include <Disks/DiskObjectStorage/ObjectStorages/GCS/gcsSettings.h>
#include <gtest/gtest.h>

using namespace DB;

/// The authentication modes of a native GCS backend are mutually exclusive, and their priority order
/// decides which credentials the client is built with -- and, for the `google_adc_*` refresh-token
/// triple, whether the transport is handed the triple to renew tokens from.

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

TEST(GCSCredentialSource, TokenBeatsRefreshTokenTriple)
{
    GCSObjectStorageSettings settings;
    settings.access_token = "token";
    settings.google_adc_client_id = "id";
    settings.google_adc_client_secret = "secret";
    settings.google_adc_refresh_token = "refresh";
    EXPECT_EQ(chooseGCSCredentialSource(settings), GCSCredentialSource::AccessToken);
}

/// The refresh-token triple is an authentication mode of its own -- the transport builds refreshable
/// credentials from it -- and the last explicit one, so anything more specific next to it still wins and
/// a stale triple cannot take over an otherwise valid configuration.
TEST(GCSCredentialSource, RefreshTokenTripleIsTheLastExplicitMode)
{
    GCSObjectStorageSettings settings;
    settings.google_adc_client_id = "id";
    settings.google_adc_client_secret = "secret";
    settings.google_adc_refresh_token = "refresh";
    EXPECT_EQ(chooseGCSCredentialSource(settings), GCSCredentialSource::RefreshToken);

    settings.no_sign_request = true;
    EXPECT_EQ(chooseGCSCredentialSource(settings), GCSCredentialSource::Anonymous);
}

/// Unlike Application Default Credentials, the triple *is* the identity: the access tokens minted from it
/// change over time but always belong to the same authorized user, so two storages carrying the same
/// triple can share one client (and therefore a server-side `RewriteObject` copy). A different token
/// endpoint is a different destination for that exchange, so it breaks the equivalence.
TEST(GCSCredentialSource, RefreshTokenTripleIsAStableClientIdentity)
{
    GCSObjectStorageSettings lhs;
    lhs.google_adc_client_id = "id";
    lhs.google_adc_client_secret = "secret";
    lhs.google_adc_refresh_token = "refresh";

    GCSObjectStorageSettings rhs = lhs;
    EXPECT_TRUE(lhs.describesSameClientAs(rhs));

    rhs.google_adc_refresh_token = "other-refresh";
    EXPECT_FALSE(lhs.describesSameClientAs(rhs));

    rhs = lhs;
    rhs.google_adc_token_uri = "https://token.example/token";
    EXPECT_FALSE(lhs.describesSameClientAs(rhs));
}

/// `google_adc_token_uri` selects nothing on its own: without a refresh token there is nothing to
/// exchange there, so accepting it silently would do nothing at all.
TEST(GCSCredentialSource, RefreshTokenTripleMustBeComplete)
{
    GCSObjectStorageSettings settings;
    settings.google_adc_client_id = "id";
    EXPECT_ANY_THROW(validateGCSRefreshTokenTriple(settings));

    settings.google_adc_client_secret = "secret";
    EXPECT_ANY_THROW(validateGCSRefreshTokenTriple(settings));

    settings.google_adc_refresh_token = "refresh";
    EXPECT_NO_THROW(validateGCSRefreshTokenTriple(settings));

    GCSObjectStorageSettings token_uri_only;
    token_uri_only.google_adc_token_uri = "https://token.example/token";
    EXPECT_ANY_THROW(validateGCSRefreshTokenTriple(token_uri_only));
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

    rhs = lhs;
    rhs.retry_attempts = lhs.retry_attempts + 1;
    EXPECT_FALSE(lhs.describesSameClientAs(rhs));
}

TEST(GCSCredentialSource, ApplicationDefaultCredentialsAreNotInterchangeable)
{
    GCSObjectStorageSettings lhs;
    GCSObjectStorageSettings rhs;
    EXPECT_FALSE(lhs.describesSameClientAs(rhs));
}

#endif
