#include <Storages/ObjectStorage/GCS/Configuration.h>

#if USE_AWS_S3 && USE_GOOGLE_CLOUD

#include <Disks/DiskObjectStorage/ObjectStorages/GCS/GCSObjectStorage.h>
#include <Disks/DiskObjectStorage/ObjectStorages/GCS/gcsSettings.h>
#include <IO/S3AuthSettings.h>
#include <Common/Exception.h>
namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace S3AuthSetting
{
    extern const S3AuthSettingsBool no_sign_request;
    extern const S3AuthSettingsBool use_environment_credentials;
    extern const S3AuthSettingsString service_account;
    extern const S3AuthSettingsString google_adc_client_id;
    extern const S3AuthSettingsString google_adc_client_secret;
    extern const S3AuthSettingsString google_adc_refresh_token;
    extern const S3AuthSettingsString access_key_id;
    extern const S3AuthSettingsString secret_access_key;
    extern const S3AuthSettingsString session_token;
    extern const S3AuthSettingsString role_arn;
    extern const S3AuthSettingsString server_side_encryption_customer_key_base64;
    extern const S3AuthSettingsString http_client;
    extern const S3AuthSettingsString metadata_service;
    extern const S3AuthSettingsString request_token_path;
    extern const S3AuthSettingsUInt64 connect_timeout_ms;
    extern const S3AuthSettingsUInt64 request_timeout_ms;
    extern const S3AuthSettingsUInt64 max_connections;
}

ObjectStoragePtr StorageGCSConfiguration::createObjectStorage(
    ContextPtr context, bool /* is_readonly */, CredentialsConfigurationCallback /* refresh_credentials_callback */)
{
    assertInitialized();

    GCSObjectStorageSettings gcs_settings;
    gcs_settings.bucket = url.bucket;
    gcs_settings.key_prefix = url.key;
    /// A non-default endpoint (e.g. the GCS emulator) is kept as a REST endpoint override.
    /// The decision is made on the exact parsed endpoint, not on a substring: a host such as
    /// `storage.googleapis.com.evil.example`, `http://storage.googleapis.com`, or a custom port
    /// must stay an override, so the client talks to the same endpoint that the URL validation saw
    /// instead of silently falling back to the real default.
    if (!isDefaultGCSEndpoint(url.endpoint))
        gcs_settings.endpoint_override = url.endpoint;

    /// A presigned URL carries its authentication in the query string (`GoogleAccessId` / `Signature`
    /// / `Expires` for V2, `X-Goog-*` for V4; `S3::URI` deliberately preserves these parameters). The
    /// S3-compatibility path forwards them with every request, but the native client authenticates
    /// with its own credentials and never sends the query parameters, so it would silently replace
    /// the signature the user supplied with the server's ambient Google identity. Fail close instead.
    for (const auto & [query_key, query_value] : url.uri.getQueryParameters())
    {
        if (query_key == "GoogleAccessId" || query_key == "Signature" || query_key == "Expires"
            || query_key == "AWSAccessKeyId" || query_key.starts_with("X-Goog-") || query_key.starts_with("X-Amz-"))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Presigned URLs (query parameter `{}`) are not supported by the native GCS backend: "
                "it authenticates with its own credentials and would ignore the URL's signature. "
                "Disable `use_native_gcs` to access the presigned URL through the S3-compatibility API",
                query_key);
    }

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
            "Use `google_adc_*` refresh-token credentials, Application Default Credentials or `NOSIGN`, "
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
    /// The metadata-service OAuth mechanism of the S3-compatibility path (`http_client = gcp_oauth`
    /// with `service_account`, `metadata_service`, `request_token_path`) requests a token for the
    /// *named* service account from a configurable metadata endpoint. Application Default Credentials
    /// only ever use the VM's default service account on the standard metadata server, so falling
    /// through to ADC would silently change the requested identity. Fail close like the checks above.
    ///
    /// `http_client` is rejected together with them, including next to a complete `google_adc_*` triple
    /// where it would select the same authorized-user flow the native client implements: without the
    /// triple it means "mint a token from the server's GCP metadata service", and honouring only one of
    /// the two shapes of one setting is a worse contract than refusing the setting outright. The triple
    /// on its own is the native form, and the message says so.
    if (!auth[S3AuthSetting::http_client].value.empty()
        || !auth[S3AuthSetting::service_account].value.empty()
        || !auth[S3AuthSetting::metadata_service].value.empty()
        || !auth[S3AuthSetting::request_token_path].value.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Metadata-service OAuth settings (`http_client`, `service_account`, `metadata_service`, `request_token_path`) "
            "are not supported by the native GCS backend; it can use a `google_adc_*` refresh-token triple on its own "
            "(remove `http_client`, the triple next to it is used directly), Application Default Credentials "
            "(which cover the default service account of the metadata server) or `NOSIGN`. "
            "Remove them or disable `use_native_gcs` to access the bucket through the S3-compatibility API");

    gcs_settings.no_sign_request = auth[S3AuthSetting::no_sign_request];

    /// The `google_adc_*` "authorized user" triple is the one explicit credential the shared argument
    /// grammar carries that the native client can use as is: the transport exchanges the refresh token
    /// for an access token and renews it as it nears expiry, so it survives a query that outlives the
    /// first token (see `ClickHouse::PocoRestAuthorizedUserOption`). `validateGCSRefreshTokenTriple`
    /// below rejects a partially specified triple.
    gcs_settings.google_adc_client_id = auth[S3AuthSetting::google_adc_client_id];
    gcs_settings.google_adc_client_secret = auth[S3AuthSetting::google_adc_client_secret];
    gcs_settings.google_adc_refresh_token = auth[S3AuthSetting::google_adc_refresh_token];

    /// `use_environment_credentials = 0` means "do not resolve an ambient, server-managed identity".
    /// On the S3-compatibility path a request with no explicit key pair and that flag off goes
    /// unsigned; on the native path the ambient identity is Application Default Credentials, so the
    /// same flag has to suppress those the same way. Silently dropping it would turn an explicit
    /// credential opt-out into "authenticate as the server", which matters most for the shape that
    /// carries it by default: `StorageS3Configuration::fromNamedCollection` sets
    /// `use_environment_credentials = 0` for a collection that only specifies a URL, precisely so it
    /// reads anonymously. The check is on the resolved credential source rather than on the flag
    /// alone, so an explicitly supplied credential still wins -- exactly as it does for S3, where the
    /// flag only decides what happens in the *absence* of one.
    if (!auth[S3AuthSetting::use_environment_credentials]
        && chooseGCSCredentialSource(gcs_settings) == GCSCredentialSource::ApplicationDefault)
        gcs_settings.no_sign_request = true;

    /// The transport knobs of the shared argument grammar are honoured by the native client too:
    /// `headers(...)` plus the `<header>` / `<access_header>` entries of the endpoint configuration
    /// (`getHeaders` decides which of them apply), the HTTP timeouts and `max_connections`.
    /// Accepting them and then
    /// talking to the endpoint with the transport's own defaults would silently change behavior of a
    /// configuration that switching `use_native_gcs` on is not supposed to affect.
    gcs_settings.headers = auth.getHeaders();
    gcs_settings.headers.insert(gcs_settings.headers.end(), headers_from_ast.begin(), headers_from_ast.end());
    gcs_settings.connect_timeout_ms = auth[S3AuthSetting::connect_timeout_ms];
    gcs_settings.request_timeout_ms = auth[S3AuthSetting::request_timeout_ms];
    gcs_settings.max_connections = auth[S3AuthSetting::max_connections];

    validateGCSRefreshTokenTriple(gcs_settings);
    checkGCSCredentialsAllowedInUserQuery(gcs_settings, context);

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
