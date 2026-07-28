#pragma once

#include "config.h"

#if USE_GOOGLE_CLOUD

#include <memory>
#include <base/types.h>
#include <Common/ObjectStorageKeyGenerator.h>
#include <IO/HTTPHeaderEntries.h>
#include <Interpreters/Context_fwd.h>

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
    /// A static OAuth2 access token (bearer). `access_token_expires_in_seconds` bounds its lifetime.
    String access_token;
    Int64 access_token_expires_in_seconds = 3600;
    /// "Authorized user" refresh-token triple (the same one the S3-compat `gcp_oauth` client uses).
    /// When set, it is exchanged for an access token via IO/GCPOAuth at load time.
    String google_adc_client_id;
    String google_adc_client_secret;
    String google_adc_refresh_token;

    /// --- HTTP transport ---

    /// Extra HTTP headers sent with every request: the `headers(...)` argument and the `<header>`
    /// entries of the endpoint configuration on the SQL surface, `<header>` entries of the disk
    /// section for a disk.
    HTTPHeaderEntries headers;
    /// TCP connection timeout of a request.
    UInt64 connect_timeout_ms = DEFAULT_GCS_CONNECT_TIMEOUT_MS;
    /// Send / receive timeout of a request.
    UInt64 request_timeout_ms = DEFAULT_GCS_REQUEST_TIMEOUT_MS;

    /// Disk-only knobs.
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
    /// A bearer access token (supplied directly, or minted from the refresh-token triple).
    AccessToken,
    /// Nothing was configured: Application Default Credentials.
    ApplicationDefault,
};

/// The single definition of the authentication priority order: both `getGCSClient` (which builds the
/// credentials) and `resolveGCSCredentialsToken` (which mints an access token only when the token is
/// the mode that wins) go through it, so the two cannot disagree.
GCSCredentialSource chooseGCSCredentialSource(const GCSObjectStorageSettings & settings);

/// If a `google_adc_*` refresh-token triple is set, and an access token is what the configuration
/// actually authenticates with, exchange the triple for an access token via IO/GCPOAuth. No-op
/// otherwise. Shared by the disk and table-function paths.
void resolveGCSCredentialsToken(GCSObjectStorageSettings & settings, const ContextPtr & context);

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
