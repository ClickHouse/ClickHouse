#pragma once

#include <Common/UnorderedSetWithMemoryTracking.h>
#include <Core/BaseSettingsFwdMacros.h>
#include <Core/SettingsEnums.h>
#include <Core/SettingsFields.h>
#include <IO/HTTPHeaderEntries.h>
#include <IO/S3/Client.h>

namespace Poco::Util
{
class AbstractConfiguration;
};

namespace DB
{
struct Settings;
struct S3AuthSettingsImpl;

/// List of available types supported in the Settings object
#define S3AUTH_SETTINGS_SUPPORTED_TYPES(CLASS_NAME, M) \
    M(CLASS_NAME, Bool) \
    M(CLASS_NAME, UInt64) \
    M(CLASS_NAME, String) \
    M(CLASS_NAME, S3UriStyle)

S3AUTH_SETTINGS_SUPPORTED_TYPES(S3AuthSettings, DECLARE_SETTING_TRAIT)

namespace S3
{

/// We use s3 settings for DiskS3, StorageS3 (StorageS3Cluster, S3Queue, etc), BackupIO_S3, etc.
/// 1. For DiskS3 we usually have configuration in disk section in configuration file.
///    REQUEST_SETTINGS, PART_UPLOAD_SETTINGS start with "s3_" prefix there, while AUTH_SETTINGS and CLIENT_SETTINGS do not
///    (does not make sense, but it happened this way).
///    If some setting is absent from disk configuration, we look up for it in the "s3." server config section,
///    where s3 settings no longer have "s3_" prefix like in disk configuration section.
///    If the settings is absent there as well, we look up for it in Users config (where query/session settings are also updated).
/// 2. For StorageS3 and similar - we look up to "s3." config section (again - settings there do not have "s3_" prefix).
///    If some setting is absent from there, we look up for it in Users config.

struct S3AuthSettings
{
    S3AuthSettings();
    S3AuthSettings(const S3AuthSettings & settings);
    S3AuthSettings(S3AuthSettings && settings) noexcept;
    S3AuthSettings(const Poco::Util::AbstractConfiguration & config, const DB::Settings & settings, const std::string & config_prefix);
    explicit S3AuthSettings(const DB::Settings & settings);
    ~S3AuthSettings();

    S3AuthSettings & operator=(S3AuthSettings && settings) noexcept;
    bool operator==(const S3AuthSettings & right);

    S3AUTH_SETTINGS_SUPPORTED_TYPES(S3AuthSettings, DECLARE_SETTING_SUBSCRIPT_OPERATOR)

    void updateFromSettings(const DB::Settings & settings, bool if_changed);
    bool hasUpdates(const S3AuthSettings & other) const;
    void updateIfChanged(const S3AuthSettings & settings);
    bool canBeUsedByUser(const String & user) const { return users.empty() || users.contains(user); }
    HTTPHeaderEntries getHeaders() const;

    /// Clear request-auth material that may have been merged in from the server `<s3>`/endpoint config (generic
    /// headers, per-request access headers, and the SSE-C key / SSE-KMS config), so a credential-restricted
    /// path that supplies its own (or no) credentials does not also send the server's headers or encryption
    /// keys to a user-chosen endpoint. Scalar credential fields (keys, role_arn, http_client, ...) are handled
    /// separately by the caller.
    void clearServerManagedRequestAuth();

    /// Clear the STS assume-role triple (`role_arn`, `role_session_name`, `external_id`). Used by restricted
    /// paths to drop a role inherited from the server `<s3>` config (so the role is never assumed with the
    /// server's identity as the STS base); the caller decides when a query-supplied role may be kept.
    void clearRoleArn();

    /// Clear the GCP OAuth metadata-service mechanism (`http_client`, `service_account`, `metadata_service`,
    /// `request_token_path`, and the explicit Application Default Credentials triple). Used by restricted paths
    /// whose syntax cannot supply these fields, so any value here came from the server `<s3>` config and would
    /// otherwise mint a server-identity bearer token for a user-chosen endpoint.
    void clearServerManagedGcpOAuth();

    HTTPHeaderEntries headers;
    HTTPHeaderEntries access_headers;

    UnorderedSetWithMemoryTracking<std::string> users;
    ServerSideEncryptionKMSConfig server_side_encryption_kms_config;

    void serialize(WriteBuffer & out, ContextPtr context) const;
    static S3AuthSettings deserialize(ReadBuffer & in, ContextPtr context);

private:
    std::unique_ptr<S3AuthSettingsImpl> impl;
};

}

}
