#pragma once

#include <Access/IAccessEntity.h>
#include <Access/ExtendedRoleSet.h>
#include <chrono>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


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
    enum ResourceType
    {
        QUERIES,        /// Number of queries.
        ERRORS,         /// Number of queries with exceptions.
        RESULT_ROWS,    /// Number of rows returned as result.
        RESULT_BYTES,   /// Number of bytes returned as result.
        READ_ROWS,      /// Number of rows read from tables.
        READ_BYTES,     /// Number of bytes read from tables.
        EXECUTION_TIME, /// Total amount of query execution time in nanoseconds.

        MAX_RESOURCE_TYPE
    };

    using ResourceAmount = UInt64;
    static constexpr ResourceAmount UNLIMITED = 0; /// 0 means unlimited.

    /// Amount of resources available to consume for each duration.
    struct Limits
    {
        ResourceAmount max[MAX_RESOURCE_TYPE];
        std::chrono::seconds duration = std::chrono::seconds::zero();

        /// Intervals can be randomized (to avoid DoS if intervals for many users end at one time).
        bool randomize_interval = false;

        Limits();
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
        CLIENT_KEY, /// Client should explicitly supply a key to use.
        CLIENT_KEY_OR_USER_NAME,  /// Same as CLIENT_KEY, but use USER_NAME if the client doesn't supply a key.
        CLIENT_KEY_OR_IP_ADDRESS, /// Same as CLIENT_KEY, but use IP_ADDRESS if the client doesn't supply a key.

        MAX
    };
    KeyType key_type = KeyType::NONE;

    /// Which roles or users should use this quota.
    ExtendedRoleSet to_roles;

    bool equal(const IAccessEntity & other) const override;
    std::shared_ptr<IAccessEntity> clone() const override { return cloneImpl<Quota>(); }

    static const char * getNameOfResourceType(ResourceType resource_type);
    static const char * resourceTypeToKeyword(ResourceType resource_type);
    static const char * resourceTypeToColumnName(ResourceType resource_type);
    static const char * getNameOfKeyType(KeyType key_type);
    static double executionTimeToSeconds(ResourceAmount ns);
    static ResourceAmount secondsToExecutionTime(double s);
};


inline const char * Quota::getNameOfResourceType(ResourceType resource_type)
{
    switch (resource_type)
    {
        case Quota::QUERIES: return "queries";
        case Quota::ERRORS: return "errors";
        case Quota::RESULT_ROWS: return "result rows";
        case Quota::RESULT_BYTES: return "result bytes";
        case Quota::READ_ROWS: return "read rows";
        case Quota::READ_BYTES: return "read bytes";
        case Quota::EXECUTION_TIME: return "execution time";
        case Quota::MAX_RESOURCE_TYPE: break;
    }
    throw Exception("Unexpected resource type: " + std::to_string(static_cast<int>(resource_type)), ErrorCodes::LOGICAL_ERROR);
}


inline const char * Quota::resourceTypeToKeyword(ResourceType resource_type)
{
    switch (resource_type)
    {
        case Quota::QUERIES: return "QUERIES";
        case Quota::ERRORS: return "ERRORS";
        case Quota::RESULT_ROWS: return "RESULT ROWS";
        case Quota::RESULT_BYTES: return "RESULT BYTES";
        case Quota::READ_ROWS: return "READ ROWS";
        case Quota::READ_BYTES: return "READ BYTES";
        case Quota::EXECUTION_TIME: return "EXECUTION TIME";
        case Quota::MAX_RESOURCE_TYPE: break;
    }
    throw Exception("Unexpected resource type: " + std::to_string(static_cast<int>(resource_type)), ErrorCodes::LOGICAL_ERROR);
}


inline const char * Quota::getNameOfKeyType(KeyType key_type)
{
    switch (key_type)
    {
        case KeyType::NONE: return "none";
        case KeyType::USER_NAME: return "user name";
        case KeyType::IP_ADDRESS: return "ip address";
        case KeyType::CLIENT_KEY: return "client key";
        case KeyType::CLIENT_KEY_OR_USER_NAME: return "client key or user name";
        case KeyType::CLIENT_KEY_OR_IP_ADDRESS: return "client key or ip address";
        case KeyType::MAX: break;
    }
    throw Exception("Unexpected quota key type: " + std::to_string(static_cast<int>(key_type)), ErrorCodes::LOGICAL_ERROR);
}


inline double Quota::executionTimeToSeconds(ResourceAmount ns)
{
    return std::chrono::duration_cast<std::chrono::duration<double>>(std::chrono::nanoseconds{ns}).count();
}

inline Quota::ResourceAmount Quota::secondsToExecutionTime(double s)
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::duration<double>(s)).count();
}


using QuotaPtr = std::shared_ptr<const Quota>;
}
