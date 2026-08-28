#include <Disks/DiskObjectStorage/ObjectStorages/GCS/gcsSettings.h>

#if USE_GOOGLE_CLOUD

#include <google/cloud/common_options.h>
#include <google/cloud/credentials.h>
#include <google/cloud/internal/curl_options.h>
#include <google/cloud/internal/rest_options.h>
#include <google/cloud/options.h>
#include <google/cloud/storage/options.h>

#include <poco_rest_options.h>

#include <algorithm>
#include <array>
#include <string_view>
#include <Poco/String.h>
#include <Poco/URI.h>
#include <Poco/Util/AbstractConfiguration.h>

#include <Common/Exception.h>
#include <Common/HTTPHeaderFilter.h>
#include <Common/Macros.h>
#include <Common/ProfileEvents.h>
#include <Common/ProxyConfigurationResolverProvider.h>
#include <Common/RemoteHostFilter.h>
#include <Common/proxyConfigurationToPocoProxyConfig.h>
#include <Core/ServerSettings.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/Context.h>

namespace gcs = ::google::cloud::storage;
namespace gc = ::google::cloud;

namespace ProfileEvents
{
    extern const Event GCSListObjects;
    extern const Event DiskGCSListObjects;
}

namespace DB
{

namespace
{

/// The JSON API addresses a listing as `GET /storage/v1/b/<bucket>/o` (`objects.list`), with the
/// prefix, page size and page token in the query string. Every other object request carries the
/// object name after `/o/`, and a bucket request has no `/o` at all, so matching the resource path
/// exactly identifies a listing page, including the ones the library fetches on its own.
bool isGCSListObjectsRequest(const std::string & method, const std::string & path_and_query)
{
    if (method != "GET")
        return false;
    const std::string_view path = std::string_view(path_and_query).substr(0, path_and_query.find('?'));
    return path.ends_with("/o");
}

}

namespace ErrorCodes
{
    extern const int ACCESS_DENIED;
    extern const int BAD_ARGUMENTS;
    extern const int INVALID_CONFIG_PARAMETER;
}

static constexpr auto DEFAULT_GCS_HOST = "storage.googleapis.com";

bool isDefaultGCSHost(const String & host)
{
    static const String default_suffix = String(".") + DEFAULT_GCS_HOST;
    return host == DEFAULT_GCS_HOST || host.ends_with(default_suffix);
}

bool isDefaultGCSEndpoint(const String & endpoint)
{
    const Poco::URI uri(endpoint);
    const auto scheme = Poco::toLower(uri.getScheme());
    const auto port = uri.getPort();
    return scheme == "https" && isDefaultGCSHost(uri.getHost()) && (port == 0 || port == 443);
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
        if (!isDefaultGCSEndpoint(endpoint))
        {
            /// A non-default endpoint means an emulator / private endpoint: keep it as an override.
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

static bool hasGCSAccessHeaders(const Poco::Util::AbstractConfiguration & config, const String & config_prefix)
{
    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(config_prefix, keys);
    return std::any_of(keys.begin(), keys.end(), [](const auto & key) { return key.starts_with("access_header"); });
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
    result.google_adc_token_uri = config.getString(config_prefix + ".google_adc_token_uri", "");

    if (!result.access_token.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "The native GCS disk does not support `access_token` because a bearer token cannot be renewed for its long-lived client. "
            "Use `google_adc_*` refresh-token credentials, Application Default Credentials, or a service-account key instead");

    if (hasGCSAccessHeaders(config, config_prefix))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "The native GCS disk does not support `access_header` because its credentials must be managed by the GCS client. "
            "Use Application Default Credentials or a service-account key instead");

    /// A disk section shares its key namespace with the `s3` disk type -- a dynamic
    /// `disk(object_storage_type = gcs, ...)` inherits the whole `s3` argument grammar -- so it can carry
    /// authentication that only the S3-compatibility path understands. The native client cannot use any of
    /// it, and accepting it silently would leave the disk authenticating as something the operator did not
    /// ask for (or writing unencrypted where they asked for SSE): the same trap as dropping
    /// `use_environment_credentials = 0` below. Name every such key and fail closed, as the SQL surface
    /// does in `StorageGCSConfiguration::createObjectStorage`.
    static constexpr std::array s3_only_auth_keys = {
        "access_key_id",
        "secret_access_key",
        "session_token",
        "role_arn",
        "role_session_name",
        "external_id",
        "http_client",
        "service_account",
        "metadata_service",
        "request_token_path",
        "server_side_encryption_customer_key_base64",
        "server_side_encryption_kms_key_id",
        "server_side_encryption_kms_encryption_context",
    };
    /// An empty value means "unset", as it does on the SQL surface -- and `forceAnonymousS3DiskConfig`
    /// writes an empty `http_client` into the resolved configuration of a disk it forces anonymous, which
    /// an `include` can leave pointing at a `gcs` backend. Only a value the operator actually set counts.
    for (const auto * s3_only_key : s3_only_auth_keys)
        if (!config.getString(config_prefix + "." + s3_only_key, "").empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "The native GCS disk does not support `{}`, which only the S3-compatibility path understands. "
                "Use `service_account_key`, `google_adc_*` refresh-token credentials, Application Default Credentials "
                "or `no_sign_request` instead, or configure an `s3` disk to reach the bucket through the "
                "S3-compatibility API", s3_only_key);

    /// `use_environment_credentials = 0` is part of the shared argument grammar a dynamic
    /// `disk(object_storage_type = gcs, ...)` inherits, and an operator may equally write it in a
    /// server-configured section. It means "do not resolve an ambient, server-managed identity",
    /// which on the native path is Application Default Credentials. Honour it as anonymous access
    /// rather than dropping it, so it cannot silently turn into "authenticate as the server".
    /// As on the S3 path, it only decides what happens in the *absence* of an explicit credential.
    if (config.has(config_prefix + ".use_environment_credentials")
        && !config.getBool(config_prefix + ".use_environment_credentials", true)
        && chooseGCSCredentialSource(result) == GCSCredentialSource::ApplicationDefault)
        result.no_sign_request = true;

    result.headers = parseGCSHeaders(config, config_prefix);
    result.connect_timeout_ms = config.getUInt64(config_prefix + ".connect_timeout_ms", DEFAULT_GCS_CONNECT_TIMEOUT_MS);
    result.request_timeout_ms = config.getUInt64(config_prefix + ".request_timeout_ms", DEFAULT_GCS_REQUEST_TIMEOUT_MS);
    result.max_connections = config.getUInt64(config_prefix + ".max_connections", DEFAULT_GCS_MAX_CONNECTIONS);

    /// The same lookup order an S3 disk uses (`S3Settings::loadFromConfigForObjectStorage`): the
    /// disk-local `<proxy>` section first, then the server-wide `<proxy>` configuration, then the
    /// `http_proxy` / `https_proxy` / `no_proxy` environment variables.
    result.proxy_resolver = ProxyConfigurationResolverProvider::getFromOldSettingsFormat(
        gcsProxyProtocol(result.endpoint_override), config_prefix, config);

    result.for_disk = true;
    result.read_only = config.getBool(config_prefix + ".readonly", false);
    result.list_object_keys_size = config.getUInt64(config_prefix + ".list_object_keys_size", 1000);

    validateGCSRefreshTokenTriple(result);

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

    /// Application Default Credentials are also read from external mutable state when the client is
    /// constructed. The same settings snapshot can therefore produce clients for different
    /// identities after that state changes. Keep cross-storage copies on the read + write path.
    if (chooseGCSCredentialSource(*this) == GCSCredentialSource::ApplicationDefault
        || chooseGCSCredentialSource(other) == GCSCredentialSource::ApplicationDefault)
        return false;

    /// The refresh-token triple, by contrast, *is* the identity: the access token minted from it varies
    /// over time but always belongs to the same authorized user, so two storages carrying the same
    /// triple (and the same token endpoint) genuinely share a client.
    if (google_adc_token_uri != other.google_adc_token_uri)
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
        && max_connections == other.max_connections
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
    if (!settings.google_adc_refresh_token.empty())
        return GCSCredentialSource::RefreshToken;
    return GCSCredentialSource::ApplicationDefault;
}

void checkGCSCredentialsAllowedInUserQuery(const GCSObjectStorageSettings & settings, const ContextPtr & context)
{
    /// Only the SQL surface (`gcs(...)` and the object storage table engine) reaches this check, and its
    /// credential carriers there are `NOSIGN` and the `google_adc_*` refresh-token triple:
    /// `StorageGCSConfiguration::createObjectStorage` rejects HMAC keys, `role_arn` and the metadata-service
    /// OAuth settings before it gets here, and a service-account key is a disk-only setting. Name only the
    /// fixes that surface actually has.
    if (chooseGCSCredentialSource(settings) == GCSCredentialSource::ApplicationDefault
        && context->shouldRestrictUserQueryS3Credentials())
        throw Exception(
            ErrorCodes::ACCESS_DENIED,
            "Native GCS access from a user query may not use Application Default Credentials because they can "
            "resolve the server's identity. Use `NOSIGN` for a public bucket, supply your own `google_adc_*` "
            "refresh-token credentials, disable `use_native_gcs` to reach the bucket through the "
            "S3-compatibility API with your own HMAC keys, or enable `s3_allow_server_credentials_in_user_queries`");
}

void validateGCSRefreshTokenTriple(const GCSObjectStorageSettings & settings)
{
    const size_t configured_adc_fields = !settings.google_adc_client_id.empty()
        + !settings.google_adc_client_secret.empty()
        + !settings.google_adc_refresh_token.empty();
    if (configured_adc_fields != 0 && configured_adc_fields != 3)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "The native GCS `google_adc_client_id`, `google_adc_client_secret`, and `google_adc_refresh_token` settings must be specified together");

    /// A token endpoint on its own selects nothing: without the triple there is no refresh token to
    /// exchange there, so accepting it would silently do nothing.
    if (!settings.google_adc_token_uri.empty() && settings.google_adc_refresh_token.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "The native GCS `google_adc_token_uri` setting only applies together with the `google_adc_*` refresh-token credentials");
}

static String readFileToString(const String & path)
{
    ReadBufferFromFile in(path);
    String contents;
    readStringUntilEOF(contents, in);
    return contents;
}

std::shared_ptr<gc::Credentials> makeGCSCredentials(const GCSObjectStorageSettings & settings)
{
    switch (chooseGCSCredentialSource(settings))
    {
        case GCSCredentialSource::Anonymous:
            return gc::MakeInsecureCredentials();
        case GCSCredentialSource::ServiceAccountKey:
            return gc::MakeServiceAccountCredentials(settings.service_account_key);
        case GCSCredentialSource::ServiceAccountKeyFile:
            return gc::MakeServiceAccountCredentials(readFileToString(settings.service_account_key_file));
        case GCSCredentialSource::AccessToken:
        {
            const auto expiry = std::chrono::system_clock::now()
                + std::chrono::seconds(std::max<Int64>(settings.access_token_expires_in_seconds, 1));
            return gc::MakeAccessTokenCredentials(settings.access_token, expiry);
        }
        case GCSCredentialSource::RefreshToken:
            /// The refresh-token triple has no counterpart in the SDK's public credential factories, so
            /// it is carried to the transport as `ClickHouse::PocoRestAuthorizedUserOption` (see
            /// `getGCSClient`), which supersedes whatever is returned here. Return the anonymous
            /// credentials rather than the Application Default ones so that if the option were ever
            /// dropped, the requests would go out unsigned and be refused — rather than silently
            /// authenticating as the server's ambient Google identity.
            return gc::MakeInsecureCredentials();
        case GCSCredentialSource::ApplicationDefault:
            /// Application Default Credentials: GOOGLE_APPLICATION_CREDENTIALS, the GCE/GKE metadata
            /// server, or the gcloud SDK configuration.
            return gc::MakeGoogleDefaultCredentials();
    }
    UNREACHABLE();
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

    options.set<gc::UnifiedCredentialsOption>(makeGCSCredentials(settings));

    if (chooseGCSCredentialSource(settings) == GCSCredentialSource::RefreshToken)
    {
        /// Hand the triple itself to the transport, which builds the SDK's `AuthorizedUserCredentials`
        /// from it. That is what renews the access token, so a long-lived disk keeps working past the
        /// first token's expiry instead of minting one token for good.
        ::ClickHouse::PocoRestAuthorizedUserOption::Type authorized_user;
        authorized_user.client_id = settings.google_adc_client_id;
        authorized_user.client_secret = settings.google_adc_client_secret;
        authorized_user.refresh_token = settings.google_adc_refresh_token;
        authorized_user.token_uri = settings.google_adc_token_uri;
        /// The refresh token is POSTed to this endpoint, so an overridden one is a second network
        /// destination the configuration picks and it goes through the same filter as the storage
        /// endpoint above. Google's own endpoint is not filtered: it is the built-in default rather
        /// than a destination the configuration chose.
        if (!authorized_user.token_uri.empty())
            context->getRemoteHostFilter().checkURL(Poco::URI(authorized_user.token_uri));
        options.set<::ClickHouse::PocoRestAuthorizedUserOption>(std::move(authorized_user));
    }

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

    /// `max_connections` of the shared argument grammar. The S3-compatibility path hands it to
    /// `maxConnections` of the AWS client configuration; the native transport bounds its per-endpoint
    /// session pool with it. Accepting the key and then keeping the transport's own default would let
    /// switching `use_native_gcs` on silently drop an operator's connection cap.
    if (settings.max_connections)
        options.set<gc::rest_internal::ConnectionPoolSizeOption>(static_cast<std::size_t>(settings.max_connections));

    /// A disk carries its own resolver (the disk section can override the server-wide proxy); the
    /// SQL surface does not, and resolves the server-wide configuration here, which is what an S3
    /// client built outside of a disk does too (`S3::ClientFactory::create`).
    auto proxy_resolver = settings.proxy_resolver;
    if (!proxy_resolver)
        proxy_resolver = ProxyConfigurationResolverProvider::get(
            gcsProxyProtocol(settings.endpoint_override), context->getConfigRef());
    options.set<::ClickHouse::PocoRestProxyConfigProviderOption>(makeGCSProxyConfigProvider(proxy_resolver));

    /// A listing is paged lazily inside `ListObjectsReader`, so the call site sees one call while the
    /// library issues one `objects.list` request per page. Count them where they are actually sent,
    /// so the counter means the same thing as the S3 and Azure per-request listing counters.
    options.set<::ClickHouse::PocoRestRequestObserverOption>(
        [for_disk = settings.for_disk](const std::string & method, const std::string & path_and_query)
        {
            if (isGCSListObjectsRequest(method, path_and_query))
            {
                ProfileEvents::increment(ProfileEvents::GCSListObjects);
                if (for_disk)
                    ProfileEvents::increment(ProfileEvents::DiskGCSListObjects);
            }
        });

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
