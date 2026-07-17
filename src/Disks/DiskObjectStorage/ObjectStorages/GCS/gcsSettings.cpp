#include <Disks/DiskObjectStorage/ObjectStorages/GCS/gcsSettings.h>

#if USE_GOOGLE_CLOUD

#include <google/cloud/credentials.h>
#include <google/cloud/options.h>
#include <google/cloud/storage/options.h>

#include <Poco/URI.h>
#include <Poco/Util/AbstractConfiguration.h>

#include <Common/Exception.h>
#include <Common/Macros.h>
#include <Core/ServerSettings.h>
#include <IO/ConnectionTimeouts.h>
#include <IO/GCPOAuth.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/Context.h>

namespace gcs = ::google::cloud::storage;
namespace gc = ::google::cloud;

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

static constexpr auto DEFAULT_GCS_HOST = "storage.googleapis.com";

void parseGCSEndpoint(const String & endpoint, String & bucket, String & key_prefix, String & endpoint_override)
{
    bucket.clear();
    key_prefix.clear();
    endpoint_override.clear();

    Poco::URI uri(endpoint);
    const auto scheme = Poco::toLower(uri.getScheme());

    if (scheme == "gs")
    {
        /// gs://bucket/prefix — the bucket is the authority component.
        bucket = uri.getHost();
        key_prefix = uri.getPath();
    }
    else if (scheme == "http" || scheme == "https")
    {
        const auto & host = uri.getHost();
        String path = uri.getPath();

        static const String default_suffix = String(".") + DEFAULT_GCS_HOST;
        if (host != DEFAULT_GCS_HOST && !host.ends_with(default_suffix))
        {
            /// A non-default host means an emulator / private endpoint: keep it as an override.
            endpoint_override = uri.getScheme() + "://" + host;
            if (uri.getPort() != 0)
                endpoint_override += ":" + std::to_string(uri.getPort());
        }

        if (host.ends_with(default_suffix))
        {
            /// Virtual-hosted style: bucket.storage.googleapis.com/key.
            bucket = host.substr(0, host.size() - default_suffix.size());
            key_prefix = path;
        }
        else
        {
            /// Path style: <host>/bucket/key.
            if (!path.empty() && path.front() == '/')
                path = path.substr(1);
            const auto slash = path.find('/');
            if (slash == String::npos)
            {
                bucket = path;
            }
            else
            {
                bucket = path.substr(0, slash);
                key_prefix = path.substr(slash + 1);
            }
        }
    }
    else
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Unsupported GCS endpoint scheme '{}' in '{}'. Expected gs://, https:// or http://", scheme, endpoint);
    }

    if (bucket.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot extract bucket name from GCS endpoint '{}'", endpoint);

    /// Normalize key prefix: no leading slash, trailing slash if non-empty.
    while (!key_prefix.empty() && key_prefix.front() == '/')
        key_prefix = key_prefix.substr(1);
    if (!key_prefix.empty() && !key_prefix.ends_with('/'))
        key_prefix.push_back('/');
}

GCSObjectStorageSettings GCSObjectStorageSettings::loadFromConfig(
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix,
    const ContextPtr & context)
{
    GCSObjectStorageSettings result;

    const String endpoint = context->getMacros()->expand(config.getString(config_prefix + ".endpoint"));
    parseGCSEndpoint(endpoint, result.bucket, result.key_prefix, result.endpoint_override);

    if (config.has(config_prefix + ".endpoint_override"))
        result.endpoint_override = config.getString(config_prefix + ".endpoint_override");

    result.no_sign_request = config.getBool(config_prefix + ".no_sign_request", false);
    result.service_account_key = config.getString(config_prefix + ".service_account_key", "");
    result.service_account_key_file = config.getString(config_prefix + ".service_account_key_file", "");
    result.access_token = config.getString(config_prefix + ".access_token", "");
    result.google_adc_client_id = config.getString(config_prefix + ".google_adc_client_id", "");
    result.google_adc_client_secret = config.getString(config_prefix + ".google_adc_client_secret", "");
    result.google_adc_refresh_token = config.getString(config_prefix + ".google_adc_refresh_token", "");

    result.read_only = config.getBool(config_prefix + ".readonly", false);
    result.list_object_keys_size = config.getUInt64(config_prefix + ".list_object_keys_size", 1000);

    resolveGCSCredentialsToken(result, context);

    return result;
}

bool GCSObjectStorageSettings::describesSameClientAs(const GCSObjectStorageSettings & other) const
{
    /// Exactly the fields consumed by `getGCSClient` to choose the endpoint and the credentials.
    /// `bucket` / `key_prefix` are intentionally excluded: two storages sharing a client may point at
    /// different buckets (that is precisely the cross-bucket rewrite case).
    return endpoint_override == other.endpoint_override
        && no_sign_request == other.no_sign_request
        && service_account_key == other.service_account_key
        && service_account_key_file == other.service_account_key_file
        && access_token == other.access_token
        && google_adc_client_id == other.google_adc_client_id
        && google_adc_client_secret == other.google_adc_client_secret
        && google_adc_refresh_token == other.google_adc_refresh_token;
}

void resolveGCSCredentialsToken(GCSObjectStorageSettings & settings, const ContextPtr & context)
{
    /// Exchange a refresh-token triple for an access token eagerly, reusing the existing S3-compat helper.
    if (!settings.access_token.empty() || settings.google_adc_refresh_token.empty())
        return;

    auto timeouts = ConnectionTimeouts::getHTTPTimeouts(context->getSettingsRef(), context->getServerSettings());
    auto token = fetchGCPOAuthToken(
        settings.google_adc_client_id, settings.google_adc_client_secret, settings.google_adc_refresh_token, timeouts);
    settings.access_token = std::move(token.access_token);
    settings.access_token_expires_in_seconds = token.expires_in;
}

static String readFileToString(const String & path)
{
    ReadBufferFromFile in(path);
    String contents;
    readStringUntilEOF(contents, in);
    return contents;
}

std::unique_ptr<gcs::Client> getGCSClient(const GCSObjectStorageSettings & settings)
{
    gc::Options options;

    std::shared_ptr<gc::Credentials> credentials;
    if (settings.no_sign_request)
    {
        credentials = gc::MakeInsecureCredentials();
    }
    else if (!settings.service_account_key.empty())
    {
        credentials = gc::MakeServiceAccountCredentials(settings.service_account_key);
    }
    else if (!settings.service_account_key_file.empty())
    {
        credentials = gc::MakeServiceAccountCredentials(readFileToString(settings.service_account_key_file));
    }
    else if (!settings.access_token.empty())
    {
        const auto expiry = std::chrono::system_clock::now()
            + std::chrono::seconds(std::max<Int64>(settings.access_token_expires_in_seconds, 1));
        credentials = gc::MakeAccessTokenCredentials(settings.access_token, expiry);
    }
    else
    {
        /// Application Default Credentials: GOOGLE_APPLICATION_CREDENTIALS, the GCE/GKE metadata
        /// server, or the gcloud SDK configuration.
        credentials = gc::MakeGoogleDefaultCredentials();
    }

    options.set<gc::UnifiedCredentialsOption>(std::move(credentials));

    if (!settings.endpoint_override.empty())
        options.set<gcs::RestEndpointOption>(settings.endpoint_override);

    return std::make_unique<gcs::Client>(std::move(options));
}

ObjectStorageKeyGeneratorPtr getGCSKeyGenerator(
    const String & key_prefix,
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix)
{
    String object_key_compatibility_prefix = config.getString(config_prefix + ".key_compatibility_prefix", String());
    String object_key_template = config.getString(config_prefix + ".key_template", String());

    Macros::MacroExpansionInfo info;
    info.ignore_unknown = true;
    info.expand_special_macros_only = true;
    info.replica = Context::getGlobalContextInstance()->getMacros()->tryGetValue("replica");
    object_key_compatibility_prefix = Context::getGlobalContextInstance()->getMacros()->expand(object_key_compatibility_prefix, info);
    info.level = 0;
    object_key_template = Context::getGlobalContextInstance()->getMacros()->expand(object_key_template, info);

    if (object_key_template.empty())
    {
        if (!object_key_compatibility_prefix.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Wrong configuration in {}. "
                            "Setting 'key_compatibility_prefix' can be defined only with setting 'key_template'.",
                            config_prefix);

        return createObjectStorageKeyGeneratorByPrefix(key_prefix);
    }

    if (!key_prefix.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Wrong configuration in {}. "
                        "Object-key prefix is forbidden with setting 'key_template', use 'key_compatibility_prefix' instead. "
                        "Prefix: '{}'.",
                        config_prefix, key_prefix);

    return createObjectStorageKeyGeneratorByTemplate(object_key_template);
}

}

#endif
