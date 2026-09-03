#pragma once

#include "config.h"

#if USE_GOOGLE_CLOUD

#include <functional>
#include <memory>
#include <base/types.h>
#include <Common/ObjectStorageKeyGenerator.h>
#include <Common/ProxyConfigurationResolver.h>
#include <IO/HTTPHeaderEntries.h>
#include <Interpreters/Context_fwd.h>

#include <Poco/Net/HTTPClientSession.h>

#include <google/cloud/credentials.h>
#include <google/cloud/storage/client.h>

namespace Poco::Util { class AbstractConfiguration; }

namespace DB
{

/// Defaults of the HTTP transport knobs. The same values as the S3 client's `connect_timeout_ms` and
/// `request_timeout_ms` (`S3::DEFAULT_CONNECT_TIMEOUT_MS` / `S3::DEFAULT_REQUEST_TIMEOUT_MS`), which
/// are what the shared `gcs()` / `ENGINE = GCS` argument grammar accepts. They are repeated here
/// instead of included, because the native backend must build without the S3 library.
inline constexpr UInt64 DEFAULT_GCS_CONNECT_TIMEOUT_MS = 1000;
inline constexpr UInt64 DEFAULT_GCS_REQUEST_TIMEOUT_MS = 30000;
/// Same value as `S3::DEFAULT_MAX_CONNECTIONS`, repeated for the same reason.
inline constexpr UInt64 DEFAULT_GCS_MAX_CONNECTIONS = 1024;
/// How many times a failed request is retried before the error is reported. The SDK's own default is
/// `LimitedTimeRetryPolicy(15 minutes)`, which sits *above* `request_timeout_ms` and does not observe
/// query cancellation, so a transient failure could keep a cancelled query retrying for a quarter of
/// an hour. A bounded number of attempts keeps the worst case a small multiple of the request timeout.
inline constexpr UInt64 DEFAULT_GCS_RETRY_ATTEMPTS = 10;

/// Parsed configuration of a native Google Cloud Storage object storage backend.
///
/// This mirrors, on the native `google-cloud-cpp` side, the subset of the S3-compatibility
/// authentication surface that makes sense for the native JSON API. HMAC access-key/secret pairs
/// are inherently an S3-XML-API concept and are not represented here — the native backend uses
/// OAuth2 / service-account / Application Default Credentials instead.
struct GCSObjectStorageSettings
{
    /// Bucket name (the GCS "namespace").
    String bucket;
    /// Common object-key prefix inside the bucket (always empty or ending with '/').
    String key_prefix;
    /// Optional REST endpoint override. Empty means the default `https://storage.googleapis.com`.
    /// Set it to point at the GCS emulator / fake-gcs-server (or `STORAGE_EMULATOR_HOST`).
    String endpoint_override;

    /// --- Authentication (mutually resolved in getGCSClient, in this priority order) ---

    /// Anonymous access (public buckets / emulator). Equivalent to the S3 `NOSIGN`.
    bool no_sign_request = false;
    /// Inline service-account JSON key.
    String service_account_key;
    /// Path to a service-account JSON key file.
    String service_account_key_file;
    /// A static OAuth2 access token (bearer). No surface sets it: a bearer token cannot be renewed, so
    /// `loadFromConfig` rejects it rather than letting a disk stop working when it expires, and the
    /// shared `s3` argument grammar has no key for it. It is still parsed (and rejected) so that an
    /// `<access_token>` in a disk section is reported instead of silently ignored.
    String access_token;
    Int64 access_token_expires_in_seconds = 3600;
    /// "Authorized user" refresh-token triple (the same one the S3-compat `gcp_oauth` client uses).
    /// The triple itself is the credential: the transport exchanges it for an access token and
    /// renews that token whenever it nears expiry (see `ClickHouse::PocoRestAuthorizedUserOption`),
    /// so it is usable by a long-lived disk as well as by a query.
    String google_adc_client_id;
    String google_adc_client_secret;
    String google_adc_refresh_token;
    /// Where the refresh token is exchanged. Empty means Google's own OAuth 2.0 endpoint
    /// (`https://oauth2.googleapis.com/token`), which is what a real deployment uses; an override is
    /// for a private token endpoint, and it is validated against `remote_url_allow_hosts` like the
    /// storage endpoint is. Disk-only: the shared `s3` argument grammar has no key for it.
    String google_adc_token_uri;

    /// --- HTTP transport ---

    /// Extra HTTP headers sent with every request: the `headers(...)` argument and the `<header>`
    /// entries of the endpoint configuration on the SQL surface, `<header>` entries of the disk
    /// section for a disk.
    HTTPHeaderEntries headers;
    /// TCP connection timeout of a request.
    UInt64 connect_timeout_ms = DEFAULT_GCS_CONNECT_TIMEOUT_MS;
    /// Send / receive timeout of a request.
    UInt64 request_timeout_ms = DEFAULT_GCS_REQUEST_TIMEOUT_MS;
    /// Upper bound on the connections the transport keeps pooled per endpoint, from the
    /// `max_connections` key of the shared argument grammar (the same key the S3-compatibility path
    /// passes to `maxConnections` of the AWS client configuration). Sessions are opened on demand and
    /// only pooled on release, so this bounds the retained ones rather than the concurrent ones —
    /// the same meaning `ConnectionPoolSizeOption` has for the upstream transports.
    UInt64 max_connections = DEFAULT_GCS_MAX_CONNECTIONS;
    /// Upper bound on the retries of one request, from the `retry_attempts` key of a disk section.
    /// 0 means "do not retry".
    UInt64 retry_attempts = DEFAULT_GCS_RETRY_ATTEMPTS;
    /// Proxy of the requests, resolved per request. Set from the disk section (the old
    /// `<gcs><proxy>` format, then the server-wide `<proxy>` / the environment) exactly like the S3
    /// disk does. Left unset on the SQL surface: `getGCSClient` then resolves the server-wide
    /// configuration itself, mirroring what `S3::ClientFactory` does for a non-disk client.
    std::shared_ptr<ProxyConfigurationResolver> proxy_resolver;

    /// Disk-only knobs.
    /// Whether this object storage backs a server-configured disk rather than the SQL surface.
    /// Only used to attribute request counters to the `DiskGCS*` profile events, mirroring how the
    /// S3 client's `isClientForDisk` splits `S3*` from `DiskS3*`.
    bool for_disk = false;
    bool read_only = false;
    /// Page size of the object listings (`maxResults`). 0 means "use the backend default".
    UInt64 list_object_keys_size = 1000;

    /// Parse the settings from a disk config section (`config_prefix`), resolving the endpoint into
    /// bucket + key prefix and (for the refresh-token flow) minting an access token.
    static GCSObjectStorageSettings loadFromConfig(
        const Poco::Util::AbstractConfiguration & config,
        const String & config_prefix,
        const ContextPtr & context);

    /// Whether these settings resolve to the same GCS endpoint and identity as `other`, so that a
    /// single `google::cloud::storage::Client` can serve both. Used to decide when a server-side
    /// `RewriteObject` copy between two GCS storages is valid (it runs through one client only).
    /// Compares exactly the fields `getGCSClient` reads to build the client, except that a
    /// `service_account_key_file` on either side always answers "no": the file contents, not the
    /// path, are the credentials, and they can be rotated in place between two client constructions.
    bool describesSameClientAs(const GCSObjectStorageSettings & other) const;
};

/// Whether `host` is the default GCS host (`storage.googleapis.com`) or one of its virtual-hosted
/// subdomains (`<bucket>.storage.googleapis.com`). The comparison is on the whole parsed host, so a
/// host merely containing that string (e.g. `storage.googleapis.com.evil.example`) does not match.
bool isDefaultGCSHost(const String & host);

/// Whether `endpoint` is the exact default GCS endpoint. A canonical host alone is not enough:
/// callers can intentionally use `http` or a custom port for a proxy or an emulator.
bool isDefaultGCSEndpoint(const String & endpoint);

/// Split a GCS endpoint into bucket, key prefix and an optional REST endpoint override.
/// Accepts `gs://bucket/prefix`, `https://storage.googleapis.com/bucket/prefix`, and
/// `http(s)://host[:port]/bucket/prefix` (the last is treated as an emulator endpoint override).
void parseGCSEndpoint(const String & endpoint, String & bucket, String & key_prefix, String & endpoint_override);

/// Which of the mutually exclusive authentication modes of GCSObjectStorageSettings wins.
enum class GCSCredentialSource
{
    /// `no_sign_request`: anonymous access.
    Anonymous,
    /// Inline service-account JSON key.
    ServiceAccountKey,
    /// Service-account JSON key read from a file.
    ServiceAccountKeyFile,
    /// A bearer access token supplied directly. Cannot be renewed.
    AccessToken,
    /// The `google_adc_*` "authorized user" refresh-token triple, exchanged for an access token by the
    /// transport, which renews it as it nears expiry.
    RefreshToken,
    /// Nothing was configured: Application Default Credentials.
    ApplicationDefault,
};

/// The single definition of the authentication priority order, so every consumer that has to know which
/// of the mutually exclusive modes a configuration selects agrees with `getGCSClient`.
GCSCredentialSource chooseGCSCredentialSource(const GCSObjectStorageSettings & settings);

/// Reject server-managed Application Default Credentials for a native GCS client reached from a
/// restricted user query. Native GCS deliberately shares the existing S3 restriction setting: both
/// credential sources can resolve the identity of the server process rather than one supplied by SQL.
void checkGCSCredentialsAllowedInUserQuery(const GCSObjectStorageSettings & settings, const ContextPtr & context);

/// Reject a `google_adc_*` refresh-token triple that is only partially specified. The three settings are
/// one credential, so two of them are always a configuration mistake rather than a mode selection.
/// Shared by the disk and table-function paths.
void validateGCSRefreshTokenTriple(const GCSObjectStorageSettings & settings);

/// Wrap a proxy resolver into the callback the Poco-based REST transport of google-cloud-cpp asks
/// for (`ClickHouse::PocoRestProxyConfigProviderOption`): every request resolves the proxy again, so
/// a rotating proxy list or a remote resolver behaves the same way it does for S3 and HTTP. Returns
/// an empty function for a null resolver, which the transport reads as "no proxy".
std::function<Poco::Net::HTTPClientSession::ProxyConfig()> makeGCSProxyConfigProvider(
    const std::shared_ptr<ProxyConfigurationResolver> & resolver);

/// Build the GCS credentials the settings authenticate with, following the priority order of
/// `chooseGCSCredentialSource`. Exposed separately from `getGCSClient` so that a consumer which
/// needs the credentials themselves — rather than a client built from them — cannot disagree with
/// the client about which authentication mode the settings select.
std::shared_ptr<google::cloud::Credentials> makeGCSCredentials(const GCSObjectStorageSettings & settings);

/// Build a native GCS storage client from the parsed settings. The resolved network destination
/// (the endpoint override, or the default GCS endpoint) is validated against the context's
/// `RemoteHostFilter` (`remote_url_allow_hosts`) before the client is constructed.
std::unique_ptr<google::cloud::storage::Client> getGCSClient(const GCSObjectStorageSettings & settings, const ContextPtr & context);

/// Build the object-storage key generator for a GCS disk (mirrors S3's getKeyGenerator, but keyed on
/// the parsed prefix instead of an S3::URI).
ObjectStorageKeyGeneratorPtr getGCSKeyGenerator(
    const String & key_prefix,
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix);

}

#endif
