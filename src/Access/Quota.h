#pragma once

#include <Access/IAccessEntity.h>
#include <Access/RolesOrUsersSet.h>
#include <chrono>


namespace DB
{

/** Quota for resources consumption for specific interval.
  * Used to limit resource usage by user.
  * Quota is applied "softly" - could be slightly exceed, because it is checked usually only on each block of processed data.
  * Accumulated values are not persisted and are lost on server restart.
  * Quota is local to server,
  *  but for distributed queries, accumulated values for read rows and bytes
  *  are collected from all participating servers and accumulated locally.
  */
struct Quota : public IAccessEntity
{
    using ResourceAmount = UInt64;

    enum ResourceType
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

        MAX_RESOURCE_TYPE
    };

    struct ResourceTypeInfo
    {
        const char * const raw_name = "";
        const String name;    /// Lowercased with underscores, e.g. "result_rows".
        const String keyword; /// Uppercased with spaces, e.g. "RESULT ROWS".
        const bool output_as_float = false;
        const UInt64 output_denominator = 1;
        String amountToString(ResourceAmount amount) const;
        ResourceAmount amountFromString(const String & str) const;
        String outputWithAmount(ResourceAmount amount) const;
        static const ResourceTypeInfo & get(ResourceType type);
    };

    /// Amount of resources available to consume for each duration.
    struct Limits
    {
        std::optional<ResourceAmount> max[MAX_RESOURCE_TYPE];
        std::chrono::seconds duration = std::chrono::seconds::zero();

        /// Intervals can be randomized (to avoid DoS if intervals for many users end at one time).
        bool randomize_interval = false;

        friend bool operator ==(const Limits & lhs, const Limits & rhs);
        friend bool operator !=(const Limits & lhs, const Limits & rhs) { return !(lhs == rhs); }
    };

    std::vector<Limits> all_limits;

    /// Key to share quota consumption.
    /// Users with the same key share the same amount of resource.
    enum class KeyType
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

    struct KeyTypeInfo
    {
        const char * const raw_name;
        const String name;  /// Lowercased with underscores, e.g. "client_key".
        const std::vector<KeyType> base_types; /// For combined types keeps base types, e.g. for CLIENT_KEY_OR_USER_NAME it keeps [KeyType::CLIENT_KEY, KeyType::USER_NAME].
        static const KeyTypeInfo & get(KeyType type);
    };

    KeyType key_type = KeyType::NONE;

    /// Which roles or users should use this quota.
    RolesOrUsersSet to_roles;

    bool equal(const IAccessEntity & other) const override;
    std::shared_ptr<IAccessEntity> clone() const override { return cloneImpl<Quota>(); }
    static constexpr const Type TYPE = Type::QUOTA;
    Type getType() const override { return TYPE; }
};

using QuotaPtr = std::shared_ptr<const Quota>;

String toString(Quota::ResourceType type);

}
