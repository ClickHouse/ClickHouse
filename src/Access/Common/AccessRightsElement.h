#pragma once

#include <Access/Common/AccessFlags.h>
#include <IO/WriteBuffer.h>
#include <tuple>


namespace DB
{
/// An element of access rights which can be represented by single line
/// GRANT ... ON ...
struct AccessRightsElement
{
    AccessFlags access_flags;

    String database;
    String table;
    Strings columns;
    String parameter;
    String filter;

    bool wildcard = false;
    bool default_database = false;

    bool grant_option = false;
    bool is_partial_revoke = false;

    AccessRightsElement() = default;
    AccessRightsElement(const AccessRightsElement &) = default;
    AccessRightsElement & operator=(const AccessRightsElement &) = default;
    AccessRightsElement(AccessRightsElement &&) = default;
    AccessRightsElement & operator=(AccessRightsElement &&) = default;

    explicit AccessRightsElement(AccessFlags access_flags_) : access_flags(access_flags_) {}

    AccessRightsElement(AccessFlags access_flags_, std::string_view database_);
    AccessRightsElement(AccessFlags access_flags_, std::string_view database_, std::string_view table_);
    AccessRightsElement(
        AccessFlags access_flags_, std::string_view database_, std::string_view table_, std::string_view column_);

    AccessRightsElement(
        AccessFlags access_flags_,
        std::string_view database_,
        std::string_view table_,
        const std::vector<std::string_view> & columns_);

    AccessRightsElement(
        AccessFlags access_flags_, std::string_view database_, std::string_view table_, const Strings & columns_);

    bool empty() const { return !access_flags || (!anyColumn() && columns.empty()); }

    bool anyDatabase() const { return database.empty() && table.empty() && !default_database; }
    bool anyTable() const { return table.empty(); }
    bool anyColumn() const { return columns.empty(); }
    bool anyParameter() const { return parameter.empty(); }
    bool hasFilter() const { return !filter.empty(); }

    auto toTuple() const { return std::tie(access_flags, default_database, database, table, columns, parameter, filter, wildcard, grant_option, is_partial_revoke); }
    friend bool operator==(const AccessRightsElement & left, const AccessRightsElement & right) { return left.toTuple() == right.toTuple(); }
    friend bool operator!=(const AccessRightsElement & left, const AccessRightsElement & right) { return !(left == right); }
    friend bool operator<(const AccessRightsElement & left, const AccessRightsElement & right) { return left.toTuple() < right.toTuple(); }

    bool sameDatabaseAndTableAndParameter(const AccessRightsElement & other) const
    {
        return sameDatabaseAndTable(other) && sameParameter(other) && (wildcard == other.wildcard) && (filter == other.filter);
    }

    bool sameParameter(const AccessRightsElement & other) const
    {
        return (parameter == other.parameter) && (anyParameter() == other.anyParameter())
            && (access_flags.getParameterType() == other.access_flags.getParameterType())
            && (isGlobalWithParameter() == other.isGlobalWithParameter());
    }

    bool sameDatabaseAndTable(const AccessRightsElement & other) const
    {
        return (database == other.database) && (table == other.table) && (anyDatabase() == other.anyDatabase()) && (anyTable() == other.anyTable());
    }

    bool sameOptions(const AccessRightsElement & other) const
    {
        return (grant_option == other.grant_option) && (is_partial_revoke == other.is_partial_revoke);
    }

    /// Returns only those flags which can be granted.
    AccessFlags getGrantableFlags() const;

    /// Throws an exception if some flags can't be granted.
    void throwIfNotGrantable() const;

    /// The filter of `GRANT READ ON S3('s3://foo/.*')` is matched with `RE2::FullMatch` when access
    /// is checked, so a pattern that does not compile never matches: the grant would look accepted
    /// and grant nothing. Throws `CANNOT_COMPILE_REGEXP` instead.
    void throwIfFilterIsNotCompilable() const;

    /// Resets flags which cannot be granted.
    void eraseNotGrantable();

    bool isEmptyDatabase() const { return database.empty() and !anyDatabase(); }

    /// If the database is empty, replaces it with `current_database`. Otherwise does nothing.
    void replaceEmptyDatabase(const String & current_database);

    /// Checks if the current access type is deprecated and replaces it with the correct one.
    void replaceDeprecated();

    void makeBackwardCompatible();

    bool isGlobalWithParameter() const { return access_flags.isGlobalWithParameter(); }

    /// Returns a human-readable representation like "GRANT SELECT, UPDATE(x, y) ON db.table".
    String toString() const;
    String toStringWithoutOptions() const;

    void formatColumnNames(WriteBuffer & buffer) const;
    void formatFilter(WriteBuffer & buffer) const;

    /// When `precise` is set, the backward-compatibility rewrites that widen a grant for the benefit of
    /// older replicas (`USER_NAME` scopes collapsed to `*.*`, `READ`/`WRITE` sources folded into `SOURCES`)
    /// are bypassed, so the element is rendered exactly. This is required for per-authentication-method
    /// `GRANTS` clauses, where widening would break the fail-close contract, and where the rewrites bring no
    /// compatibility benefit anyway because older replicas do not understand the clause at all.
    void formatONClause(WriteBuffer & buffer, bool precise = false) const;
};


/// Multiple elements of access rights.
class AccessRightsElements : public std::vector<AccessRightsElement>
{
public:
    using Base = std::vector<AccessRightsElement>;
    using Base::Base;

    bool empty() const;

    /// Whether the list literally contains no elements. This differs from empty(), which is semantic
    /// and also returns true when the elements grant no access (e.g. a single `USAGE ON *.*`). Use this
    /// to tell "no clause was written" apart from "the clause was written but grants nothing".
    bool structurallyEmpty() const { return Base::empty(); }
    bool sameDatabaseAndTableAndParameter() const;
    bool sameDatabaseAndTable() const;
    bool sameOptions() const;

    /// Throws an exception if some flags can't be granted.
    void throwIfNotGrantable() const;

    /// For each element throws if its filter is not a compilable regular expression.
    void throwIfFilterIsNotCompilable() const;

    /// Resets flags which cannot be granted.
    void eraseNotGrantable();

    /// For each element checks if the current access type is deprecated and replaces it with the correct one.
    void replaceDeprecated();

    /// If the database is empty, replaces it with `current_database`. Otherwise does nothing.
    void replaceEmptyDatabase(const String & current_database);

    /// Returns a human-readable representation like "GRANT SELECT, UPDATE(x, y) ON db.table".
    String toString() const;
    String toStringWithoutOptions() const;

    /// See `AccessRightsElement::formatONClause`: `precise` bypasses the backward-compatibility widening,
    /// which is mandatory for per-authentication-method `GRANTS` clauses.
    void formatElementsWithoutOptions(WriteBuffer & buffer, bool precise = false) const;

    /// Precise serialization without the backward-compatibility widening, matching `SHOW CREATE USER` and
    /// `system.users.auth_grants`. Use this (never `toString`) to derive a stable identity for an
    /// auth-method `GRANTS` limit — e.g. as part of the async-insert queue key or the query-result-cache
    /// key — because the widening in `toString`/`toStringWithoutOptions` collapses distinct source-level
    /// limits such as `READ ON FILE` and `WRITE ON FILE` into one under `enable_read_write_grants = 0`.
    String toStringPrecise() const;
};

}
