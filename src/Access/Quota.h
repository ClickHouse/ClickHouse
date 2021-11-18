#pragma once

#include <Access/IAccessEntity.h>
#include <Access/Common/QuotaDefs.h>
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
    /// Amount of resources available to consume for each duration.
    struct Limits
    {
        std::optional<QuotaValue> max[static_cast<size_t>(QuotaType::MAX)];
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
