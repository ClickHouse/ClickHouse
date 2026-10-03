#pragma once

#include <Core/Types.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>

#include <boost/noncopyable.hpp>

#include <vector>


namespace DB
{

class IUserDefinedSQLObjectsStorage;

/// Prepares a `CREATE TYPE` query for storing: the `IF NOT EXISTS` / `OR REPLACE` flags are stripped,
/// so the stored definition does not depend on how the type was created.
ASTPtr normalizeCreateTypeQuery(const IAST & create_type_query);

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
    /// number of arguments; and when it would make the type depend on itself. Replacing a type that other types
    /// use is only allowed if the new definition keeps the number of parameters those uses rely on.
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

private:
    UserDefinedTypeFactory() = default;

    /// The storage of the server, or nullptr when there is no global context (e.g. in tools that never load types).
    const IUserDefinedSQLObjectsStorage * tryGetStorage() const;
};

}
