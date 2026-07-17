#pragma once

#include "config.h"

#if USE_GOOGLE_CLOUD

#include <memory>
#include <base/types.h>
#include <Common/ObjectStorageKeyGenerator.h>
#include <Interpreters/Context_fwd.h>

#include <google/cloud/storage/client.h>

namespace Poco::Util { class AbstractConfiguration; }

namespace DB
{

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

    /// Disk-only knobs.
    bool read_only = false;
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
    /// Compares exactly the fields `getGCSClient` reads to build the client.
    bool describesSameClientAs(const GCSObjectStorageSettings & other) const;
};

/// Split a GCS endpoint into bucket, key prefix and an optional REST endpoint override.
/// Accepts `gs://bucket/prefix`, `https://storage.googleapis.com/bucket/prefix`, and
/// `http(s)://host[:port]/bucket/prefix` (the last is treated as an emulator endpoint override).
void parseGCSEndpoint(const String & endpoint, String & bucket, String & key_prefix, String & endpoint_override);

/// If a `google_adc_*` refresh-token triple is set and no access token has been resolved yet, exchange
/// it for an access token via IO/GCPOAuth. No-op otherwise. Shared by the disk and table-function paths.
void resolveGCSCredentialsToken(GCSObjectStorageSettings & settings, const ContextPtr & context);

/// Build a native GCS storage client from the parsed settings.
std::unique_ptr<google::cloud::storage::Client> getGCSClient(const GCSObjectStorageSettings & settings);

/// Build the object-storage key generator for a GCS disk (mirrors S3's getKeyGenerator, but keyed on
/// the parsed prefix instead of an S3::URI).
ObjectStorageKeyGeneratorPtr getGCSKeyGenerator(
    const String & key_prefix,
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix);

}

#endif
