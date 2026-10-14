#pragma once

#include <Core/Types.h>


namespace DB
{
/// We use UInt64 to count used resources.
using QuotaValue = UInt64;

/// Kinds of resource what we wish to quota.
enum class QuotaType : uint8_t
{
    QUERIES,                                /// Number of queries.
    QUERY_SELECTS,                          /// Number of select queries.
    QUERY_INSERTS,                          /// Number of insert queries.
    ERRORS,                                 /// Number of queries with exceptions.
    RESULT_ROWS,                            /// Number of rows returned as result.
    RESULT_BYTES,                           /// Number of bytes returned as result.
    READ_ROWS,                              /// Number of rows read from tables.
    READ_BYTES,                             /// Number of bytes read from tables.
    EXECUTION_TIME,                         /// Total amount of query execution time in nanoseconds.
    WRITTEN_BYTES,                          /// Number of bytes written to tables.
    FAILED_SEQUENTIAL_AUTHENTICATIONS,      /// Number of recent failed authentications.

    MAX
};

String toString(QuotaType type);

struct QuotaTypeInfo
{
    const char * const raw_name = "";
    const String name;    /// Lowercased with underscores, e.g. "result_rows".
    const String keyword; /// Uppercased with spaces, e.g. "RESULT ROWS".
    const String current_usage_description;
    const String max_allowed_usage_description;
    const bool output_as_float = false;
    const UInt64 output_denominator = 1;
    String valueToString(QuotaValue value) const;
    QuotaValue stringToValue(const String & str) const;
    String valueToStringWithName(QuotaValue value) const;
    static const QuotaTypeInfo & get(QuotaType type);
};

/// Key to share quota consumption.
/// Users with the same key share the same amount of resource.
enum class QuotaKeyType : uint8_t
{
    NONE,       /// All users share the same quota.
    USER_NAME,  /// Connections with the same user name share the same quota.
    IP_ADDRESS, /// Connections from the same IP share the same quota.
    FORWARDED_IP_ADDRESS, /// Use X-Forwarded-For HTTP header instead of IP address.
    CLIENT_KEY, /// Client should explicitly supply a key to use.
    CLIENT_KEY_OR_USER_NAME,  /// Same as CLIENT_KEY, but use USER_NAME if the client doesn't supply a key.
    CLIENT_KEY_OR_IP_ADDRESS, /// Same as CLIENT_KEY, but use IP_ADDRESS if the client doesn't supply a key.

    MAX
};

String toString(QuotaKeyType type);

struct QuotaKeyTypeInfo
{
    const char * const raw_name;
    const String name;  /// Lowercased with underscores, e.g. "client_key".
    const std::vector<QuotaKeyType> base_types; /// For combined types keeps base types, e.g. for CLIENT_KEY_OR_USER_NAME it keeps [KeyType::CLIENT_KEY, KeyAccessEntityType::USER_NAME].
    static const QuotaKeyTypeInfo & get(QuotaKeyType type);
};

}
