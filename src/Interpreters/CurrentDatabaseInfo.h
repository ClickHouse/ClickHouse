#pragma once

#include <base/types.h>

#include <string_view>


namespace DB
{

/// The current database as selected by `USE`: a plain database (`db`) or a database with a table
/// namespace path (`db.ns`, `db.ns.sub`). `value` is the full name, unquoted; `separator_idx` is
/// the position of the dot after the database part, or `String::npos` when no namespace is selected.
/// The split is frozen when the current database is set, never re-derived from the catalog.
struct CurrentDatabaseInfo
{
    CurrentDatabaseInfo() = default;

    /// Splits at the first dot: "db.ns.sub" -> "db" + "ns.sub". A quoted first component ('a.b' or
    /// "a.b", escaped as `quoteString` / `doubleQuoteString` write it) is one literal database name and
    /// is stored unquoted; a leading or trailing dot separates nothing.
    explicit CurrentDatabaseInfo(String full_name_);

    /// `db` or `db.ns`, what `USE` received and what query logs and the `database` setting show
    const String & getFullName() const { return value; }
    /// the physical database, `db`
    std::string_view getDatabasePart() const { return std::string_view(value).substr(0, separator_idx); }
    /// the namespace path without the database, `ns` or `ns.sub`; empty when none
    std::string_view getTablePrefixPart() const
    {
        return hasTablePrefix() ? std::string_view(value).substr(separator_idx + 1) : std::string_view{};
    }
    bool hasTablePrefix() const { return separator_idx != String::npos; }
    bool empty() const { return value.empty(); }

    bool operator==(const CurrentDatabaseInfo &) const = default;

private:
    String value;
    size_t separator_idx = String::npos;
};

}
