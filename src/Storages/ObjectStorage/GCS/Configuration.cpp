#include <Storages/ObjectStorage/GCS/Configuration.h>

#if USE_AWS_S3 && USE_GOOGLE_CLOUD

#include <Disks/DiskObjectStorage/ObjectStorages/GCS/GCSObjectStorage.h>
#include <Disks/DiskObjectStorage/ObjectStorages/GCS/gcsSettings.h>
#include <IO/S3AuthSettings.h>
#include <Common/Exception.h>
#include <Poco/URI.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace S3AuthSetting
{
    extern const S3AuthSettingsBool no_sign_request;
    extern const S3AuthSettingsString service_account;
    extern const S3AuthSettingsString google_adc_client_id;
    extern const S3AuthSettingsString google_adc_client_secret;
    extern const S3AuthSettingsString google_adc_refresh_token;
    extern const S3AuthSettingsString access_key_id;
    extern const S3AuthSettingsString secret_access_key;
    extern const S3AuthSettingsString session_token;
    extern const S3AuthSettingsString role_arn;
    extern const S3AuthSettingsString server_side_encryption_customer_key_base64;
}

ObjectStoragePtr StorageGCSConfiguration::createObjectStorage(
    ContextPtr context, bool /* is_readonly */, CredentialsConfigurationCallback /* refresh_credentials_callback */)
{
    assertInitialized();

    GCSObjectStorageSettings gcs_settings;
    gcs_settings.bucket = url.bucket;
    gcs_settings.key_prefix = url.key;
    /// A non-default endpoint (e.g. the GCS emulator) is kept as a REST endpoint override.
    /// The decision is made on the exact parsed host, not on a substring: a host such as
    /// `storage.googleapis.com.evil.example` must stay an override, so the client talks to the same
    /// host that the URL validation saw instead of silently falling back to the real default.
    if (!isDefaultGCSHost(Poco::URI(url.endpoint).getHost()))
        gcs_settings.endpoint_override = url.endpoint;

    const auto & auth = s3_settings->auth_settings;

    /// The argument grammar is shared with `s3(...)`, so a query or named collection can supply
    /// authentication that only the S3-compatibility path understands (HMAC keys, an STS role,
    /// server-side encryption keys). The native client cannot use any of it: silently discarding it
    /// would make the request authenticate as the server's ambient Google identity (Application
    /// Default Credentials) instead of the credentials the user supplied, and would write data
    /// unencrypted where the user asked for SSE. Fail close instead of changing semantics.
    if (!auth[S3AuthSetting::access_key_id].value.empty()
        || !auth[S3AuthSetting::secret_access_key].value.empty()
        || !auth[S3AuthSetting::session_token].value.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "HMAC key credentials are not supported by the native GCS backend. "
            "Use `google_adc_*` settings, a service account key, or `NOSIGN`, "
            "or disable `use_native_gcs` to access the bucket through the S3-compatibility API");
    if (!auth[S3AuthSetting::role_arn].value.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "`role_arn` (STS role assumption) is not supported by the native GCS backend. "
            "Remove it or disable `use_native_gcs` to access the bucket through the S3-compatibility API");
    if (!auth[S3AuthSetting::server_side_encryption_customer_key_base64].value.empty()
        || auth.server_side_encryption_kms_config != S3::ServerSideEncryptionKMSConfig{})
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "S3 server-side encryption settings are not supported by the native GCS backend. "
            "Remove them or disable `use_native_gcs` to access the bucket through the S3-compatibility API");

    gcs_settings.no_sign_request = auth[S3AuthSetting::no_sign_request];
    gcs_settings.google_adc_client_id = auth[S3AuthSetting::google_adc_client_id];
    gcs_settings.google_adc_client_secret = auth[S3AuthSetting::google_adc_client_secret];
    gcs_settings.google_adc_refresh_token = auth[S3AuthSetting::google_adc_refresh_token];
    /// `service_account` (metadata-service SA) has no dedicated native factory: Application Default
    /// Credentials already cover the GCE/GKE metadata server, so it falls through to the default path.

    resolveGCSCredentialsToken(gcs_settings, context);

    auto client = getGCSClient(gcs_settings, context);
    const auto description = url.endpoint;

    return std::make_shared<GCSObjectStorage>(
        std::move(client),
        std::move(gcs_settings),
        description,
        /* key_generator */ nullptr,
        "StorageGCS");
}

}

#endif
