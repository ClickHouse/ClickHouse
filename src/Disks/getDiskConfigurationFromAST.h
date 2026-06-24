#pragma once

#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/DOM/AutoPtr.h>
#include <Poco/Util/XMLConfiguration.h>
#include <Parsers/IAST_fwd.h>
#include <Interpreters/Context_fwd.h>

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

/// What the pre-resolution (AST-level) check in `getDiskConfigurationFromASTImpl` found, needed to finish the
/// server-managed S3 credential check once `include`/`from_env`/`from_zk` are resolved.
struct DynamicS3DiskCredentialInfo
{
    /// The pre-resolution check decided the disk must be loaded anonymously (metadata load only); the caller
    /// must pass the loaded configuration to `forceAnonymousS3DiskConfig`.
    bool load_anonymously = false;
    /// The AST used `include`, so the resolved configuration may differ from the AST and its credential
    /// provenance cannot be trusted (an include can supply the type and credentials, e.g. via `from_env`).
    bool has_include = false;
    /// The AST itself supplied a safe credential form (an explicit literal key pair, `no_sign_request`, an
    /// explicit `use_environment_credentials = 0` without `role_arn`, or a complete explicit Google ADC
    /// triple). Only these count as user-provided; values an `include` injects do not.
    bool ast_has_explicit_credentials = false;
};

/// The same as above function, but return XML::Document for easier modification of result configuration.
/// When `is_loading_from_existing_metadata` is true (loading from existing metadata — server restart,
/// force-restore, or `UNDROP TABLE`), the `dynamic_disk_allow_*` restrictions are skipped because the disk
/// configuration was already validated when the object was originally created. User-initiated `ATTACH TABLE`
/// / `ATTACH DATABASE` queries pass false so those restrictions apply.
///
/// The server-managed S3 credential restriction (see `Context::shouldRestrictUserQueryS3Credentials`) is
/// applied even on metadata load. The pre-resolution outcome is reported through `*info`; after the caller
/// resolves `include`/`from_env`/`from_zk` and loads the configuration, it must call
/// `forceAnonymousS3DiskConfig` (when `info->load_anonymously`) or `validateResolvedS3DiskCredentials`.
[[ maybe_unused ]] Poco::AutoPtr<Poco::XML::Document> getDiskConfigurationFromASTImpl(const ASTs & disk_args, ContextPtr context, bool is_loading_from_existing_metadata = false, DynamicS3DiskCredentialInfo * info = nullptr);

/// Rewrite a dynamic disk configuration so its S3 client is built anonymously (see `getDiskConfigurationFromASTImpl`).
void forceAnonymousS3DiskConfig(Poco::Util::AbstractConfiguration & config);

/// Re-apply the server-managed S3 credential restriction to a dynamic disk configuration after `include`
/// (and `from_env`/`from_zk`) are resolved, so an `include` cannot inject an S3 backend with server-managed
/// auth (or credentials of untrusted provenance) past the pre-resolution check. For an `include`d disk whose
/// resolved backend is S3, only credentials the AST itself supplied (`info.ast_has_explicit_credentials`)
/// count as safe; otherwise it throws `ACCESS_DENIED`, or forces the disk anonymous when loading from
/// existing metadata (governed by `s3_load_table_anonymously_if_credentials_restricted`).
void validateResolvedS3DiskCredentials(Poco::Util::AbstractConfiguration & config, ContextPtr context, bool is_loading_from_existing_metadata, const DynamicS3DiskCredentialInfo & info);

/*
 * A reverse function.
 */
[[ maybe_unused ]] ASTs convertDiskConfigurationToAST(const Poco::Util::AbstractConfiguration & configuration, const std::string & config_path);

}
