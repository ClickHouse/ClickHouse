#pragma once

#include <Core/Types.h>


namespace DB
{

/// Represents the full name of a row policy, e.g. "myfilter ON mydb.mytable".
struct RowPolicyName
{
    static constexpr char ANY_TABLE_MARK[] = "";

    String short_name;
    String database;
    String table_name;

    bool empty() const { return short_name.empty(); }
    String toString() const;
    auto toTuple() const { return std::tie(short_name, database, table_name); }
    friend bool operator ==(const RowPolicyName & left, const RowPolicyName & right) { return left.toTuple() == right.toTuple(); }
    friend bool operator !=(const RowPolicyName & left, const RowPolicyName & right) { return left.toTuple() != right.toTuple(); }
};


/// Types of the filters of row policies.
/// Currently only RowPolicyFilterType::SELECT is supported.
enum class RowPolicyFilterType : uint8_t
{
    /// Filter is a SQL conditional expression used to figure out which rows should be visible
    /// for user or available for modification. If the expression returns NULL or false for some rows
    /// those rows are silently suppressed.
    SELECT_FILTER,

#if 0 /// Row-level security for INSERT, UPDATE, DELETE is not implemented yet.
    /// Check is a SQL condition expression used to check whether a row can be written into
    /// the table. If the expression returns NULL or false an exception is thrown.
    /// If a conditional expression here is empty it means no filtering is applied.
    INSERT_CHECK,
    UPDATE_FILTER,
    UPDATE_CHECK,
    DELETE_FILTER,
#endif

    MAX
};

String toString(RowPolicyFilterType type);

struct RowPolicyFilterTypeInfo
{
    const char * const raw_name;
    const String name;    /// Lowercased with underscores, e.g. "select_filter".
    const String command; /// Uppercased without last word, e.g. "SELECT".
    const String description;
    const bool is_check;  /// E.g. false for SELECT_FILTER.
    static const RowPolicyFilterTypeInfo & get(RowPolicyFilterType type);
};

}
