#pragma once

#include <Core/Types.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>

#include <boost/noncopyable.hpp>

#include <mutex>
#include <unordered_map>
#include <vector>


namespace DB
{

class IUserDefinedSQLObjectsStorage;
class BackupEntriesCollector;
class RestorerFromBackup;

/// Prepares a `CREATE TYPE` query for storing: the `IF NOT EXISTS` / `OR REPLACE` flags are stripped,
/// so the stored definition does not depend on how the type was created.
ASTPtr normalizeCreateTypeQuery(const IAST & create_type_query);

/// A copy of the type expression `ast` with every type name that is a key of `substitutions` (and has no arguments)
/// replaced with a copy of the corresponding value: the expansion of a parameterized user-defined type.
ASTPtr substituteTypeParameters(const ASTPtr & ast, const std::unordered_map<String, ASTPtr> & substitutions);

/// The registry of user-defined types, created with
///     CREATE TYPE name[(parameters)] AS base_type
/// A user-defined type is a named, optionally parameterized alias for a data type expression: `DataTypeFactory`
/// expands it to `base_type` (with the actual arguments substituted for the parameters) whenever the type is
/// resolved, so tables store the expanded built-in type, and the alias is never needed to read them back.
///
/// The definitions are the normalized `CREATE TYPE` queries and are persisted in the user-defined SQL objects
/// storage of the server (on disk or in ZooKeeper, like user-defined functions), see
/// `Context::getUserDefinedTypesStorage`.
class UserDefinedTypeFactory final : private boost::noncopyable
{
public:
    static UserDefinedTypeFactory & instance();

    /// Validates the definition and stores the type. Returns false if the type already exists and the caller
    /// asked neither to throw nor to replace it (`IF NOT EXISTS`).
    ///
    /// The definition is rejected when the name is a built-in type, alias or type family; when it references an
    /// unknown type, a known type family with a wrong number of arguments, or a user-defined type with a wrong
    /// number of arguments; when a parameterized definition uses a type family with a number of arguments the family
    /// never accepts (`CREATE TYPE Broken(T) AS Map(T)`); and when it would make the type depend on itself. Replacing a type that other types
    /// use is only allowed if the new definition keeps the number of parameters those uses rely on
    /// and stays valid for the actual arguments they use it with.
    bool registerType(
        const ContextMutablePtr & current_context,
        const String & type_name,
        const ASTPtr & create_type_query,
        bool throw_if_exists,
        bool replace_if_exists) const;

    /// Removes the type. Rejected while another user-defined type references it. Returns false if the type does
    /// not exist and `throw_if_not_exists` is false (`IF EXISTS`).
    bool unregisterType(const ContextMutablePtr & current_context, const String & type_name, bool throw_if_not_exists) const;

    /// The normalized `CREATE TYPE` query (an `ASTCreateTypeQuery`) of the type, or nullptr if there is no such type.
    ASTPtr tryGet(const String & type_name) const;

    /// Same as `tryGet`, but throws `UNKNOWN_TYPE` if there is no such type.
    ASTPtr get(const String & type_name) const;

    bool has(const String & type_name) const;

    /// The names of all user-defined types, sorted.
    std::vector<String> getAllRegisteredNames() const;

    /// Makes backup entries for all user-defined types (`BACKUP TABLE system.user_defined_types`).
    void backup(BackupEntriesCollector & backup_entries_collector, const String & data_path_in_backup) const;

    /// Restores user-defined types from the backup. The `create_function` restore setting decides what happens to the
    /// types that already exist. Every definition is validated against the set of types after the restore before any
    /// of them is stored, and the stored ones are reverted if storing another one fails.
    void restore(RestorerFromBackup & restorer, const String & data_path_in_backup) const;

private:
    UserDefinedTypeFactory() = default;

    /// The storage of the server, or nullptr when there is no global context (e.g. in tools that never load types).
    const IUserDefinedSQLObjectsStorage * tryGetStorage() const;

    /// Validates the definition against the current types and returns the normalized query to store, or nullptr if
    /// the type already exists and is kept (`IF NOT EXISTS`). Must be called with `ddl_mutex` locked.
    ASTPtr prepareType(
        const IUserDefinedSQLObjectsStorage & storage,
        const String & type_name,
        const ASTPtr & create_type_query,
        bool throw_if_exists,
        bool replace_if_exists) const;

    static bool storeType(
        const ContextMutablePtr & current_context,
        IUserDefinedSQLObjectsStorage & storage,
        const String & type_name,
        const ASTPtr & normalized_query,
        bool throw_if_exists,
        bool replace_if_exists);

    static void syncWithStorage(IUserDefinedSQLObjectsStorage & storage);

    /// Serializes the changes of the types with the validation they depend on: otherwise `DROP TYPE A` could check
    /// that no type uses `A` while another query validates and stores `CREATE TYPE B AS A`, and both would succeed.
    mutable std::mutex ddl_mutex;
};

}
