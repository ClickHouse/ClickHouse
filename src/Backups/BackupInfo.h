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

    /// Gets the named collection specified by id_arg, checks access rights,
    /// and applies any key-value overrides from kv_args.
    /// Returns nullptr if id_arg is empty (i.e., no named collection is used).
    NamedCollectionPtr getNamedCollection(ContextPtr context) const;
};

}
