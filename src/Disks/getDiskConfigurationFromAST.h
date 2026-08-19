#pragma once

#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/DOM/AutoPtr.h>
#include <Poco/Util/XMLConfiguration.h>
#include <Parsers/IAST_fwd.h>
#include <Interpreters/Context_fwd.h>

#include <unordered_map>

namespace DB
{

using DiskConfigurationPtr = Poco::AutoPtr<Poco::Util::AbstractConfiguration>;

/**
 * Transform a list of pairs ( key1=value1, key2=value2, ... ), where keys and values are ASTLiteral or ASTIdentifier
 * into
 * <disk>
 *     <key1>value1</key1>
 *     <key2>value2</key2>
 *     ...
 * </disk>
 *
 * Used in case disk configuration is passed via AST when creating
 * a disk object on-the-fly without any configuration file.
 */
DiskConfigurationPtr getDiskConfigurationFromAST(const ASTs & disk_args, ContextPtr context);

/// What the pre-resolution (AST-level) credential check found, to finish the check once `include` is resolved.
struct DynamicS3DiskCredentialInfo
{
    /// The pre-resolution check decided the disk must be loaded anonymously (see `forceAnonymousS3DiskConfig`).
    bool load_anonymously = false;
    /// The AST used `include`, so the resolved credentials' provenance cannot be trusted.
    bool has_include = false;
    /// The disk backend type comes from `from_env` or `from_zk`, so its S3 credential check must run after
    /// substitutions are resolved. This prevents the conservative pre-resolution S3 check from rejecting a
    /// native GCS disk that has safe native credentials.
    bool has_indirect_backend_type = false;
    /// The pre-resolution check exempted this disk from the restriction (e.g. a server-internal
    /// `system`-database disk, or a persisted `_server_credentials_allowed` marker), so the post-`include`
    /// re-check must not re-apply it.
    bool restriction_exempt = false;
    /// On a fresh create the disk relies on server-managed credentials and the session opted in, so the caller
    /// should persist a `_server_credentials_allowed` marker in the stored definition. The marker is honored
    /// (only) on later metadata loads, so the disk keeps working across restart without re-opting in.
    bool persist_server_credentials_allowance = false;
    /// The safe credential forms the AST itself supplied with literal values; the resolved auth mode is
    /// validated against these.
    bool ast_has_explicit_key_pair = false;                 /// literal `access_key_id` + `secret_access_key`
    bool ast_has_no_sign_request = false;                   /// literal `no_sign_request`
    bool ast_has_use_environment_credentials_off = false;   /// literal `use_environment_credentials = 0`, no `role_arn`
    bool ast_has_explicit_gcp_adc = false;                  /// complete literal Google ADC triple

    /// For the post-`include` GCS re-check (`validateResolvedGCSDiskCredentials`): whether the AST itself
    /// supplied each native GCS credential field as a literal value. A resolved field the AST did not vouch
    /// for came from the included (server-side) configuration, i.e. server-managed auth material.
    bool for_system_database = false;                       /// server-internal disk, exempt like the AST-level GCS checks
    bool ast_has_gcs_service_account_key = false;
    bool ast_has_gcs_access_token = false;
    bool ast_has_google_adc_client_id = false;
    bool ast_has_google_adc_client_secret = false;
    bool ast_has_google_adc_refresh_token = false;
    /// Literal native GCS authentication headers supplied by the SQL AST. The post-`include` validation
    /// compares these with the resolved configuration so an unrelated `include` does not reject them.
    std::unordered_map<String, String> ast_gcs_headers;
};

/// The same as above function, but return XML::Document for easier modification of result configuration.
/// When `is_loading_from_existing_metadata` is true (loading from existing metadata — server restart,
/// force-restore, or `UNDROP TABLE`), the `dynamic_disk_allow_*` restrictions are skipped because the disk
/// configuration was already validated when the object was originally created. User-initiated `ATTACH TABLE`
/// / `ATTACH DATABASE` queries pass false so those restrictions apply.
///
/// The server-managed S3 credential restriction is applied even on metadata load. The pre-resolution outcome
/// is reported through `*info`; after resolving `include`, the caller must call `forceAnonymousS3DiskConfig`
/// (when `info->load_anonymously`) or `validateResolvedS3DiskCredentials`.
[[ maybe_unused ]] Poco::AutoPtr<Poco::XML::Document> getDiskConfigurationFromASTImpl(const ASTs & disk_args, ContextPtr context, bool is_loading_from_existing_metadata = false, DynamicS3DiskCredentialInfo * info = nullptr, bool for_system_database = false);

/// Rewrite a dynamic disk configuration so its S3 client is built anonymously (see `getDiskConfigurationFromASTImpl`).
void forceAnonymousS3DiskConfig(Poco::Util::AbstractConfiguration & config);

/// As `forceAnonymousS3DiskConfig`, but for a single backend selected by `prefix` (empty for the disk root, or
/// `locations.<name>.` for one child of a multi-location `DiskObjectStorage`).
void forceAnonymousS3DiskConfigAtPrefix(Poco::Util::AbstractConfiguration & config, const String & prefix);

/// Re-apply the credential restriction after `include` is resolved, so an `include` cannot inject an S3
/// backend with server-managed auth past the pre-resolution check. Throws `ACCESS_DENIED`, or forces the disk
/// anonymous when loading from existing metadata (see `s3_load_table_anonymously_if_credentials_restricted`).
void validateResolvedS3DiskCredentials(Poco::Util::AbstractConfiguration & config, ContextPtr context, bool is_loading_from_existing_metadata, const DynamicS3DiskCredentialInfo & info);

/// Resolve the native GCS backend after `include`, `from_env`, and `from_zk` are processed. This enforces the
/// experimental feature gate on the actual backend, then re-applies GCS credential restrictions after `include`
/// so it cannot inject a `gcs` backend with `service_account_key_file` (a server-side file read),
/// server-managed credential fields, or authentication headers past the pre-resolution AST checks. Credential
/// fields and included headers are always rejected: a session-only credential opt-in cannot be persisted for
/// included values. The ADC check follows `s3_allow_server_credentials_in_user_queries` and throws
/// `ACCESS_DENIED` (fail-closed) when restricted.
void validateResolvedGCSDiskCredentials(
    const Poco::Util::AbstractConfiguration & config,
    ContextPtr context,
    bool is_loading_from_existing_metadata,
    const DynamicS3DiskCredentialInfo & info);

/*
 * A reverse function.
 */
[[ maybe_unused ]] ASTs convertDiskConfigurationToAST(const Poco::Util::AbstractConfiguration & configuration, const std::string & config_path);

}
