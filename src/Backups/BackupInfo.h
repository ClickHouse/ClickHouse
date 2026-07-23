#pragma once

#include <Common/NamedCollections/NamedCollections_fwd.h>
#include <Core/Field.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>

#include <memory>

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
    NamedCollectionPtr frozen_named_collection;
    std::shared_ptr<const BackupInfo> credentials_source;

    String toString() const;
    static BackupInfo fromString(const String & str);

    ASTPtr toAST() const;
    static BackupInfo fromAST(const IAST & ast);

    String toStringForLogging() const;
    static String evaluateKeyValueArgument(const ASTPtr & kv_arg, size_t index, ContextPtr context);
    bool isEquivalentTo(const BackupInfo & other, ContextPtr context) const;

    /// Gets the named collection specified by id_arg, checks access rights,
    /// and applies any key-value overrides from kv_args.
    /// Returns nullptr if id_arg is empty (i.e., no named collection is used).
    NamedCollectionPtr getNamedCollection(ContextPtr context) const;

    /// Stores a private copy of the resolved named collection so later identity generation
    /// and backup creation use the same collection state.
    BackupInfo freezeNamedCollection(ContextPtr context) const;
};

}
