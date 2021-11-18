#pragma once

#include <Core/Types.h>


namespace DB
{
using QuotaValue = UInt64;

enum class QuotaType
{
    QUERIES,        /// Number of queries.
    QUERY_SELECTS,  /// Number of select queries.
    QUERY_INSERTS,  /// Number of inserts queries.
    ERRORS,         /// Number of queries with exceptions.
    RESULT_ROWS,    /// Number of rows returned as result.
    RESULT_BYTES,   /// Number of bytes returned as result.
    READ_ROWS,      /// Number of rows read from tables.
    READ_BYTES,     /// Number of bytes read from tables.
    EXECUTION_TIME, /// Total amount of query execution time in nanoseconds.

    MAX
};

struct QuotaTypeInfo
{
    const char * const raw_name = "";
    const String name;    /// Lowercased with underscores, e.g. "result_rows".
    const String keyword; /// Uppercased with spaces, e.g. "RESULT ROWS".
    const bool output_as_float = false;
    const UInt64 output_denominator = 1;
    String valueToString(QuotaValue value) const;
    QuotaValue stringToValue(const String & str) const;
    String valueToStringWithName(QuotaValue value) const;
    static const QuotaTypeInfo & get(QuotaType type);
};

String toString(QuotaType type);

}
