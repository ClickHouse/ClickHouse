#include <Disks/DiskObjectStorage/ObjectStorages/GCS/gcsSettings.h>

#if USE_GOOGLE_CLOUD

#include <google/cloud/common_options.h>
#include <google/cloud/credentials.h>
#include <google/cloud/internal/rest_options.h>
#include <google/cloud/options.h>
#include <google/cloud/storage/options.h>

#include <poco_rest_options.h>

#include <Poco/URI.h>
#include <Poco/Util/AbstractConfiguration.h>

#include <Common/Exception.h>
#include <Common/HTTPHeaderFilter.h>
#include <Common/Macros.h>
#include <Common/ProxyConfigurationResolverProvider.h>
#include <Common/RemoteHostFilter.h>
#include <Common/proxyConfigurationToPocoProxyConfig.h>
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
    extern const int INVALID_CONFIG_PARAMETER;
}

static constexpr auto DEFAULT_GCS_HOST = "storage.googleapis.com";

bool isDefaultGCSHost(const String & host)
{
    static const String default_suffix = String(".") + DEFAULT_GCS_HOST;
    return host == DEFAULT_GCS_HOST || host.ends_with(default_suffix);
}

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
        if (!isDefaultGCSHost(host))
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

/// `<header>Name: value</header>` entries of a config section, in the same form the `<s3>` sections
/// accept them (`IO/S3Common.h`'s `getHTTPHeaders`, which the native backend cannot use because it
/// must build without the S3 library).
static HTTPHeaderEntries parseGCSHeaders(const Poco::Util::AbstractConfiguration & config, const String & config_prefix)
{
    HTTPHeaderEntries headers;
    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(config_prefix, keys);
    for (const auto & key : keys)
    {
        if (!key.starts_with("header"))
            continue;

        const auto header = config.getString(config_prefix + "." + key);
        const auto delimiter = header.find(':');
        if (delimiter == String::npos)
            throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "Malformed header value in {}.{}", config_prefix, key);
        headers.emplace_back(header.substr(0, delimiter), header.substr(delimiter + 1));
    }
    return headers;
}

/// The protocol a proxy has to be resolved for: the scheme the client will actually speak, which is
/// the scheme of the endpoint override when there is one and `https` (the default GCS endpoint)
/// otherwise. It selects between the `<proxy><http>` and `<proxy><https>` sections, exactly as the
/// scheme of the S3 endpoint does for an S3 disk.
static ProxyConfiguration::Protocol gcsProxyProtocol(const String & endpoint_override)
{
    if (endpoint_override.empty())
        return ProxyConfiguration::Protocol::HTTPS;
    return ProxyConfiguration::protocolFromString(Poco::toLower(Poco::URI(endpoint_override).getScheme()));
}

std::function<Poco::Net::HTTPClientSession::ProxyConfig()> makeGCSProxyConfigProvider(
    const std::shared_ptr<ProxyConfigurationResolver> & resolver)
{
    if (!resolver)
        return {};

    return [resolver] { return proxyConfigurationToPocoProxyConfig(resolver->resolve()); };
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

    result.headers = parseGCSHeaders(config, config_prefix);
    result.connect_timeout_ms = config.getUInt64(config_prefix + ".connect_timeout_ms", DEFAULT_GCS_CONNECT_TIMEOUT_MS);
    result.request_timeout_ms = config.getUInt64(config_prefix + ".request_timeout_ms", DEFAULT_GCS_REQUEST_TIMEOUT_MS);

    /// The same lookup order an S3 disk uses (`S3Settings::loadFromConfigForObjectStorage`): the
    /// disk-local `<proxy>` section first, then the server-wide `<proxy>` configuration, then the
    /// `http_proxy` / `https_proxy` / `no_proxy` environment variables.
    result.proxy_resolver = ProxyConfigurationResolverProvider::getFromOldSettingsFormat(
        gcsProxyProtocol(result.endpoint_override), config_prefix, config);

    result.read_only = config.getBool(config_prefix + ".readonly", false);
    result.list_object_keys_size = config.getUInt64(config_prefix + ".list_object_keys_size", 1000);

    resolveGCSCredentialsToken(result, context);

    return result;
}

bool GCSObjectStorageSettings::describesSameClientAs(const GCSObjectStorageSettings & other) const
{
    /// A service-account key *file* identifies the credentials only indirectly: `getGCSClient` reads
    /// its contents when it builds the client, so two snapshots naming the same path can still hold
    /// clients of two different service accounts if the key was rotated in place between the two
    /// client constructions. Refuse to treat them as interchangeable — the cost is that a
    /// cross-storage copy falls back to read + write instead of a server-side `RewriteObject`.
    if (!service_account_key_file.empty() || !other.service_account_key_file.empty())
        return false;

    /// Exactly the fields consumed by `getGCSClient` to build the client: the endpoint, the
    /// credentials, and the transport knobs. `bucket` / `key_prefix` are intentionally excluded: two
    /// storages sharing a client may point at different buckets (that is precisely the cross-bucket
    /// rewrite case).
    return endpoint_override == other.endpoint_override
        && no_sign_request == other.no_sign_request
        && service_account_key == other.service_account_key
        && service_account_key_file == other.service_account_key_file
        && access_token == other.access_token
        && google_adc_client_id == other.google_adc_client_id
        && google_adc_client_secret == other.google_adc_client_secret
        && google_adc_refresh_token == other.google_adc_refresh_token
        && headers == other.headers
        && connect_timeout_ms == other.connect_timeout_ms
        && request_timeout_ms == other.request_timeout_ms
        /// Resolvers are compared by identity: two of them can hand out different proxies (and a
        /// remote one cannot be asked what it would answer without querying it), so only the very
        /// same resolver object is known to describe the same transport. The cost of the
        /// conservative answer is a copy that falls back to read + write.
        && proxy_resolver == other.proxy_resolver;
}

GCSCredentialSource chooseGCSCredentialSource(const GCSObjectStorageSettings & settings)
{
    if (settings.no_sign_request)
        return GCSCredentialSource::Anonymous;
    if (!settings.service_account_key.empty())
        return GCSCredentialSource::ServiceAccountKey;
    if (!settings.service_account_key_file.empty())
        return GCSCredentialSource::ServiceAccountKeyFile;
    if (!settings.access_token.empty())
        return GCSCredentialSource::AccessToken;
    return GCSCredentialSource::ApplicationDefault;
}

void resolveGCSCredentialsToken(GCSObjectStorageSettings & settings, const ContextPtr & context)
{
    if (settings.google_adc_refresh_token.empty())
        return;

    /// The refresh-token triple is the lowest-priority authentication mode, so a configuration that
    /// also carries a higher-priority one (anonymous access, a service-account key, an access token
    /// supplied directly) never authenticates with the minted token. Minting it anyway would make
    /// such a configuration fail for a reason that does not apply to it — e.g. a bucket accessed with
    /// `no_sign_request` would stop working because of a stale `google_adc_*` triple next to it.
    if (chooseGCSCredentialSource(settings) != GCSCredentialSource::ApplicationDefault)
        return;

    /// Exchange the refresh-token triple for an access token eagerly, reusing the existing S3-compat helper.

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

std::unique_ptr<gcs::Client> getGCSClient(const GCSObjectStorageSettings & settings, const ContextPtr & context)
{
    /// Fail-closed validation of the actual network destination against `remote_url_allow_hosts`,
    /// mirroring what the S3, Azure and web object storage transports do before a user-configurable
    /// endpoint is used. Throws when a filter is configured and the host is not allowed.
    const String resolved_endpoint = settings.endpoint_override.empty()
        ? String("https://") + DEFAULT_GCS_HOST
        : settings.endpoint_override;
    context->getRemoteHostFilter().checkURL(Poco::URI(resolved_endpoint));

    gc::Options options;

    std::shared_ptr<gc::Credentials> credentials;
    switch (chooseGCSCredentialSource(settings))
    {
        case GCSCredentialSource::Anonymous:
            credentials = gc::MakeInsecureCredentials();
            break;
        case GCSCredentialSource::ServiceAccountKey:
            credentials = gc::MakeServiceAccountCredentials(settings.service_account_key);
            break;
        case GCSCredentialSource::ServiceAccountKeyFile:
            credentials = gc::MakeServiceAccountCredentials(readFileToString(settings.service_account_key_file));
            break;
        case GCSCredentialSource::AccessToken:
        {
            const auto expiry = std::chrono::system_clock::now()
                + std::chrono::seconds(std::max<Int64>(settings.access_token_expires_in_seconds, 1));
            credentials = gc::MakeAccessTokenCredentials(settings.access_token, expiry);
            break;
        }
        case GCSCredentialSource::ApplicationDefault:
            /// Application Default Credentials: GOOGLE_APPLICATION_CREDENTIALS, the GCE/GKE metadata
            /// server, or the gcloud SDK configuration.
            credentials = gc::MakeGoogleDefaultCredentials();
            break;
    }

    options.set<gc::UnifiedCredentialsOption>(std::move(credentials));

    if (!settings.endpoint_override.empty())
        options.set<gcs::RestEndpointOption>(settings.endpoint_override);

    if (!settings.headers.empty())
    {
        /// The server-wide `<http_forbid_headers>` filter applies to every header the client will
        /// send, whatever surface supplied it — `headers(...)` in a query, an endpoint `<header>` /
        /// `<access_header>` entry, or a disk `<header>` entry. The S3 transports validate their
        /// final header set before the client is built; do the same here so switching to the native
        /// backend cannot smuggle a forbidden header past the filter. checkAndNormalizeHeaders
        /// mutates (normalizes) the entries, so run it on the copy the client options are built from.
        auto headers = settings.headers;
        context->getHTTPHeaderFilter().checkAndNormalizeHeaders(headers);

        gc::CustomHeadersOption::Type custom_headers;
        for (const auto & header : headers)
            custom_headers.emplace(header.name, header.value);
        options.set<gc::CustomHeadersOption>(std::move(custom_headers));
    }

    /// The transport is the Poco-based one from `contrib/google-cloud-cpp-cmake/poco_rest_client.cc`:
    /// there, the stall timeouts are the send and receive timeouts of the request, which is what the
    /// S3-compatibility path means by `request_timeout_ms`. The upstream options are whole seconds, so
    /// round up to keep a sub-second timeout from becoming "no timeout".
    const auto request_timeout = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::milliseconds(settings.request_timeout_ms) + std::chrono::milliseconds(999));
    options.set<gc::rest_internal::TransferStallTimeoutOption>(request_timeout);
    options.set<gc::rest_internal::DownloadStallTimeoutOption>(request_timeout);
    options.set<::ClickHouse::PocoRestConnectTimeoutOption>(std::chrono::milliseconds(settings.connect_timeout_ms));

    /// A disk carries its own resolver (the disk section can override the server-wide proxy); the
    /// SQL surface does not, and resolves the server-wide configuration here, which is what an S3
    /// client built outside of a disk does too (`S3::ClientFactory::create`).
    auto proxy_resolver = settings.proxy_resolver;
    if (!proxy_resolver)
        proxy_resolver = ProxyConfigurationResolverProvider::get(
            gcsProxyProtocol(settings.endpoint_override), context->getConfigRef());
    options.set<::ClickHouse::PocoRestProxyConfigProviderOption>(makeGCSProxyConfigProvider(proxy_resolver));

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
