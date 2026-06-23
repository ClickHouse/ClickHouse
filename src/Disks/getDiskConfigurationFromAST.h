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

/// The same as above function, but return XML::Document for easier modification of result configuration.
/// When `is_loading_from_existing_metadata` is true (loading from existing metadata — server restart,
/// force-restore, or `UNDROP TABLE`), the `dynamic_disk_allow_*` restrictions are skipped because the disk
/// configuration was already validated when the object was originally created. User-initiated `ATTACH TABLE`
/// / `ATTACH DATABASE` queries pass false so those restrictions apply.
///
/// The server-managed S3 credential restriction (see `Context::shouldRestrictUserQueryS3Credentials`) is
/// applied even on metadata load. If the definition would resolve server-managed credentials there, the
/// function does not throw but sets `*load_anonymously` to true; the caller must then pass the loaded
/// configuration to `forceAnonymousS3DiskConfig` so the disk is built anonymous (inaccessible) instead of
/// using the server's identity. See the server setting `s3_load_table_anonymously_if_credentials_restricted`.
[[ maybe_unused ]] Poco::AutoPtr<Poco::XML::Document> getDiskConfigurationFromASTImpl(const ASTs & disk_args, ContextPtr context, bool is_loading_from_existing_metadata = false, bool * load_anonymously = nullptr);

/// Rewrite a dynamic disk configuration so its S3 client is built anonymously (see `getDiskConfigurationFromASTImpl`).
void forceAnonymousS3DiskConfig(Poco::Util::AbstractConfiguration & config);

/*
 * A reverse function.
 */
[[ maybe_unused ]] ASTs convertDiskConfigurationToAST(const Poco::Util::AbstractConfiguration & configuration, const std::string & config_path);

}
