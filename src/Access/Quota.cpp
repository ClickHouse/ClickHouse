#include <Access/Quota.h>
#include <ext/range.h>
#include <boost/range/algorithm/equal.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


bool operator ==(const Quota::Limits & lhs, const Quota::Limits & rhs)
{
    return boost::range::equal(lhs.max, rhs.max) && (lhs.duration == rhs.duration)
        && (lhs.randomize_interval == rhs.randomize_interval);
}

bool Quota::equal(const IAccessEntity & other) const
{
    if (!IAccessEntity::equal(other))
        return false;
    const auto & other_quota = typeid_cast<const Quota &>(other);
    return (all_limits == other_quota.all_limits) && (key_type == other_quota.key_type) && (to_roles == other_quota.to_roles);
}


String Quota::ResourceTypeInfo::amountToString(ResourceAmount amount) const
{
    if (!(amount % output_denominator))
        return std::to_string(amount / output_denominator);
    else
        return boost::lexical_cast<std::string>(static_cast<double>(amount) / output_denominator);
}

Quota::ResourceAmount Quota::ResourceTypeInfo::amountFromString(const String & str) const
{
    if (output_denominator == 1)
        return static_cast<ResourceAmount>(std::strtoul(str.c_str(), nullptr, 10));
    else
        return static_cast<ResourceAmount>(std::strtod(str.c_str(), nullptr) * output_denominator);
}

String Quota::ResourceTypeInfo::outputWithAmount(ResourceAmount amount) const
{
    String res = name;
    res += " = ";
    res += amountToString(amount);
    return res;
}

String toString(Quota::ResourceType type)
{
    return Quota::ResourceTypeInfo::get(type).raw_name;
}

const Quota::ResourceTypeInfo & Quota::ResourceTypeInfo::get(ResourceType type)
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


String toString(Quota::KeyType type)
{
    return Quota::KeyTypeInfo::get(type).raw_name;
}

const Quota::KeyTypeInfo & Quota::KeyTypeInfo::get(KeyType type)
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
                for (auto kt : ext::range(KeyType::MAX))
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

}

