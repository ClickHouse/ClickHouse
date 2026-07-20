#include <Storages/ObjectStorage/GCS/Configuration.h>

#if USE_AWS_S3 && USE_GOOGLE_CLOUD

#include <Disks/DiskObjectStorage/ObjectStorages/GCS/GCSObjectStorage.h>
#include <Disks/DiskObjectStorage/ObjectStorages/GCS/gcsSettings.h>
#include <IO/S3AuthSettings.h>
#include <Poco/URI.h>

namespace DB
{

namespace S3AuthSetting
{
    extern const S3AuthSettingsBool no_sign_request;
    extern const S3AuthSettingsString service_account;
    extern const S3AuthSettingsString google_adc_client_id;
    extern const S3AuthSettingsString google_adc_client_secret;
    extern const S3AuthSettingsString google_adc_refresh_token;
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
