#pragma once

#include <Access/IAccessEntity.h>
#include <Access/RolesOrUsersSet.h>
#include <base/range.h>
#include <boost/algorithm/string/split.hpp>
#include <boost/lexical_cast.hpp>
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


inline String Quota::ResourceTypeInfo::amountToString(ResourceAmount amount) const
{
    if (!(amount % output_denominator))
        return std::to_string(amount / output_denominator);
    else
        return boost::lexical_cast<std::string>(static_cast<double>(amount) / output_denominator);
}

inline Quota::ResourceAmount Quota::ResourceTypeInfo::amountFromString(const String & str) const
{
    if (output_denominator == 1)
        return static_cast<ResourceAmount>(std::strtoul(str.c_str(), nullptr, 10));
    else
        return static_cast<ResourceAmount>(std::strtod(str.c_str(), nullptr) * output_denominator);
}

inline String Quota::ResourceTypeInfo::outputWithAmount(ResourceAmount amount) const
{
    String res = name;
    res += " = ";
    res += amountToString(amount);
    return res;
}

inline String toString(Quota::ResourceType type)
{
    return Quota::ResourceTypeInfo::get(type).raw_name;
}

inline const Quota::ResourceTypeInfo & Quota::ResourceTypeInfo::get(ResourceType type)
{
    static constexpr auto make_info = [](const char * raw_name_, UInt64 output_denominator_)
    {
        String init_name = raw_name_;
        boost::to_lower(init_name);
        String init_keyword = raw_name_;
        boost::replace_all(init_keyword, "_", " ");
        bool init_output_as_float = (output_denominator_ != 1);
        return ResourceTypeInfo{raw_name_, std::move(init_name), std::move(init_keyword), init_output_as_float, output_denominator_};
    };

    switch (type)
    {
        case Quota::QUERIES:
        {
            static const auto info = make_info("QUERIES", 1);
            return info;
        }
        case Quota::QUERY_SELECTS:
        {
            static const auto info = make_info("QUERY_SELECTS", 1);
            return info;
        }
        case Quota::QUERY_INSERTS:
        {
            static const auto info = make_info("QUERY_INSERTS", 1);
            return info;
        }
        case Quota::ERRORS:
        {
            static const auto info = make_info("ERRORS", 1);
            return info;
        }
        case Quota::RESULT_ROWS:
        {
            static const auto info = make_info("RESULT_ROWS", 1);
            return info;
        }
        case Quota::RESULT_BYTES:
        {
            static const auto info = make_info("RESULT_BYTES", 1);
            return info;
        }
        case Quota::READ_ROWS:
        {
            static const auto info = make_info("READ_ROWS", 1);
            return info;
        }
        case Quota::READ_BYTES:
        {
            static const auto info = make_info("READ_BYTES", 1);
            return info;
        }
        case Quota::EXECUTION_TIME:
        {
            static const auto info = make_info("EXECUTION_TIME", 1000000000 /* execution_time is stored in nanoseconds */);
            return info;
        }
        case Quota::MAX_RESOURCE_TYPE: break;
    }
    throw Exception("Unexpected resource type: " + std::to_string(static_cast<int>(type)), ErrorCodes::LOGICAL_ERROR);
}


inline String toString(Quota::KeyType type)
{
    return Quota::KeyTypeInfo::get(type).raw_name;
}

inline const Quota::KeyTypeInfo & Quota::KeyTypeInfo::get(KeyType type)
{
    static constexpr auto make_info = [](const char * raw_name_)
    {
        String init_name = raw_name_;
        boost::to_lower(init_name);
        std::vector<KeyType> init_base_types;
        String replaced = boost::algorithm::replace_all_copy(init_name, "_or_", "|");
        Strings tokens;
        boost::algorithm::split(tokens, replaced, boost::is_any_of("|"));
        if (tokens.size() > 1)
        {
            for (const auto & token : tokens)
            {
                for (auto kt : collections::range(KeyType::MAX))
                {
                    if (KeyTypeInfo::get(kt).name == token)
                    {
                        init_base_types.push_back(kt);
                        break;
                    }
                }
            }
        }
        return KeyTypeInfo{raw_name_, std::move(init_name), std::move(init_base_types)};
    };

    switch (type)
    {
        case KeyType::NONE:
        {
            static const auto info = make_info("NONE");
            return info;
        }
        case KeyType::USER_NAME:
        {
            static const auto info = make_info("USER_NAME");
            return info;
        }
        case KeyType::IP_ADDRESS:
        {
            static const auto info = make_info("IP_ADDRESS");
            return info;
        }
        case KeyType::FORWARDED_IP_ADDRESS:
        {
            static const auto info = make_info("FORWARDED_IP_ADDRESS");
            return info;
        }
        case KeyType::CLIENT_KEY:
        {
            static const auto info = make_info("CLIENT_KEY");
            return info;
        }
        case KeyType::CLIENT_KEY_OR_USER_NAME:
        {
            static const auto info = make_info("CLIENT_KEY_OR_USER_NAME");
            return info;
        }
        case KeyType::CLIENT_KEY_OR_IP_ADDRESS:
        {
            static const auto info = make_info("CLIENT_KEY_OR_IP_ADDRESS");
            return info;
        }
        case KeyType::MAX: break;
    }
    throw Exception("Unexpected quota key type: " + std::to_string(static_cast<int>(type)), ErrorCodes::LOGICAL_ERROR);
}

using QuotaPtr = std::shared_ptr<const Quota>;
}
