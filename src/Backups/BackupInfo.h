#pragma once

#include <Common/NamedCollections/NamedCollections_fwd.h>
#include <Core/Field.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

/// Information about a backup.
struct BackupInfo
{
    String backup_engine_name;
    String id_arg;
    std::vector<Field> args;
    ASTPtr function_arg;
    ASTs kv_args;

    String toString() const;
    static BackupInfo fromString(const String & str);

    ASTPtr toAST() const;
    static BackupInfo fromAST(const IAST & ast);

    String toStringForLogging() const;

    void copyS3CredentialsTo(BackupInfo & dest) const;

    /// Whether `copyS3CredentialsTo` would succeed (both sides are `S3` without named collections
    /// and this backup locator carries explicit credentials).
    bool canCopyS3CredentialsTo(const BackupInfo & dest) const;

    /// Returns a copy without the `S3` credential arguments: S3('url', 'access_key_id',
    /// 'secret_access_key') becomes S3('url'), and credential key-value arguments are removed.
    /// Used to serialize the base backup locator into the `.backup` metadata, which must never
    /// contain credentials; on restore they are taken from the restore source locator, from the
    /// `base_backup` setting, or from the server-side configuration.
    /// The context is used to evaluate non-literal `url` overrides the same way `getNamedCollection`
    /// does; without a context such overrides are rejected.
    BackupInfo withoutS3Credentials(ContextPtr context = nullptr) const;

    /// Gets the named collection specified by id_arg, checks access rights,
    /// and applies any key-value overrides from kv_args.
    /// Returns nullptr if id_arg is empty (i.e., no named collection is used).
    NamedCollectionPtr getNamedCollection(ContextPtr context) const;
};

}
