#include <string.h>

#include <Poco/RegularExpression.h>
#include <Poco/Net/IPAddress.h>
#include <Poco/Net/SocketAddress.h>
#include <Poco/Net/DNS.h>
#include <Poco/Util/Application.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/String.h>

#include <Common/Exception.h>
#include <IO/ReadHelpers.h>
#include <IO/HexWriteBuffer.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Common/SimpleCache.h>
#include <Common/StringUtils.h>
#include <Interpreters/Users.h>

#include <openssl/sha.h>

#include <common/logger_useful.h>

#include <ext/scope_guard.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int DNS_ERROR;
    extern const int UNKNOWN_ADDRESS_PATTERN_TYPE;
    extern const int UNKNOWN_USER;
    extern const int REQUIRED_PASSWORD;
    extern const int WRONG_PASSWORD;
    extern const int IP_ADDRESS_NOT_ALLOWED;
    extern const int BAD_ARGUMENTS;
}


static Poco::Net::IPAddress toIPv6(const Poco::Net::IPAddress addr)
{
    if (addr.family() == Poco::Net::IPAddress::IPv6)
        return addr;

    return Poco::Net::IPAddress("::FFFF:" + addr.toString());
}


/// IP-address or subnet mask. Example: 213.180.204.3 or 10.0.0.1/8 or 2a02:6b8::3 or 2a02:6b8::3/64.
class IPAddressPattern : public IAddressPattern
{
private:
    /// Address of mask. Always transformed to IPv6.
    Poco::Net::IPAddress mask_address;
    /// Number of bits in mask.
    UInt8 prefix_bits;

public:
    explicit IPAddressPattern(const String & str)
    {
        const char * pos = strchr(str.c_str(), '/');

        if (nullptr == pos)
        {
            construct(Poco::Net::IPAddress(str));
        }
        else
        {
            String addr(str, 0, pos - str.c_str());
            UInt8 prefix_bits_ = parse<UInt8>(pos + 1);

            construct(Poco::Net::IPAddress(addr), prefix_bits_);
        }
    }

    bool contains(const Poco::Net::IPAddress & addr) const override
    {
        return prefixBitsEquals(reinterpret_cast<const char *>(toIPv6(addr).addr()), reinterpret_cast<const char *>(mask_address.addr()), prefix_bits);
    }

private:
    void construct(const Poco::Net::IPAddress & mask_address_)
    {
        mask_address = toIPv6(mask_address_);
        prefix_bits = 128;
    }

    void construct(const Poco::Net::IPAddress & mask_address_, UInt8 prefix_bits_)
    {
        mask_address = toIPv6(mask_address_);
        prefix_bits = mask_address_.family() == Poco::Net::IPAddress::IPv4
            ? prefix_bits_ + 96
            : prefix_bits_;
    }

    static bool prefixBitsEquals(const char * lhs, const char * rhs, UInt8 prefix_bits)
    {
        UInt8 prefix_bytes = prefix_bits / 8;
        UInt8 remaining_bits = prefix_bits % 8;

        return 0 == memcmp(lhs, rhs, prefix_bytes)
            && (remaining_bits % 8 == 0
                 || (lhs[prefix_bytes] >> (8 - remaining_bits)) == (rhs[prefix_bytes] >> (8 - remaining_bits)));
    }
};


/// Check that address equals to one of hostname addresses.
class HostExactPattern : public IAddressPattern
{
private:
    String host;

    static bool containsImpl(const String & host, const Poco::Net::IPAddress & addr)
    {
        Poco::Net::IPAddress addr_v6 = toIPv6(addr);

        /// Resolve by hand, because Poco don't use AI_ALL flag but we need it.
        addrinfo * ai = nullptr;

        addrinfo hints;
        memset(&hints, 0, sizeof(hints));
        hints.ai_family = AF_UNSPEC;
        hints.ai_flags |= AI_V4MAPPED | AI_ALL;

        int ret = getaddrinfo(host.c_str(), nullptr, &hints, &ai);
        if (0 != ret)
            throw Exception("Cannot getaddrinfo: " + std::string(gai_strerror(ret)), ErrorCodes::DNS_ERROR);

        SCOPE_EXIT(
        {
            freeaddrinfo(ai);
        });

        for (; ai != nullptr; ai = ai->ai_next)
        {
            if (ai->ai_addrlen && ai->ai_addr)
            {
                if (ai->ai_family == AF_INET6)
                {
                    if (addr_v6 == Poco::Net::IPAddress(
                        &reinterpret_cast<sockaddr_in6*>(ai->ai_addr)->sin6_addr, sizeof(in6_addr),
                        reinterpret_cast<sockaddr_in6*>(ai->ai_addr)->sin6_scope_id))
                    {
                        return true;
                    }
                }
                else if (ai->ai_family == AF_INET)
                {
                    if (addr_v6 == toIPv6(Poco::Net::IPAddress(
                        &reinterpret_cast<sockaddr_in*>(ai->ai_addr)->sin_addr, sizeof(in_addr))))
                    {
                        return true;
                    }
                }
            }
        }

        return false;
    }

public:
    explicit HostExactPattern(const String & host_) : host(host_) {}

    bool contains(const Poco::Net::IPAddress & addr) const override
    {
        static SimpleCache<decltype(containsImpl), &containsImpl> cache;
        return cache(host, addr);
    }
};


/// Check that PTR record for address match the regexp (and in addition, check that PTR record is resolved back to client address).
class HostRegexpPattern : public IAddressPattern
{
private:
    Poco::RegularExpression host_regexp;

    static String getDomain(const Poco::Net::IPAddress & addr)
    {
        Poco::Net::SocketAddress sock_addr(addr, 0);

        /// Resolve by hand, because Poco library doesn't have such functionality.
        char domain[1024];
        int gai_errno = getnameinfo(sock_addr.addr(), sock_addr.length(), domain, sizeof(domain), nullptr, 0, NI_NAMEREQD);
        if (0 != gai_errno)
            throw Exception("Cannot getnameinfo: " + std::string(gai_strerror(gai_errno)), ErrorCodes::DNS_ERROR);

        return domain;
    }

public:
    explicit HostRegexpPattern(const String & host_regexp_) : host_regexp(host_regexp_) {}

    bool contains(const Poco::Net::IPAddress & addr) const override
    {
        static SimpleCache<decltype(getDomain), &getDomain> cache;

        String domain = cache(addr);
        Poco::RegularExpression::Match match;

        if (host_regexp.match(domain, match) && HostExactPattern(domain).contains(addr))
            return true;

        return false;
    }
};



bool AddressPatterns::contains(const Poco::Net::IPAddress & addr) const
{
    for (size_t i = 0, size = patterns.size(); i < size; ++i)
    {
        /// If host cannot be resolved, skip it and try next.
        try
        {
            if (patterns[i]->contains(addr))
                return true;
        }
        catch (const DB::Exception & e)
        {
            LOG_WARNING(&Logger::get("AddressPatterns"),
                "Failed to check if pattern contains address " << addr.toString() << ". " << e.displayText() << ", code = " << e.code());

            if (e.code() == ErrorCodes::DNS_ERROR)
            {
                continue;
            }
            else
                throw;
        }
    }

    return false;
}

void AddressPatterns::addFromConfig(const String & config_elem, Poco::Util::AbstractConfiguration & config)
{
    Poco::Util::AbstractConfiguration::Keys config_keys;
    config.keys(config_elem, config_keys);

    for (Poco::Util::AbstractConfiguration::Keys::const_iterator it = config_keys.begin(); it != config_keys.end(); ++it)
    {
        Container::value_type pattern;
        String value = config.getString(config_elem + "." + *it);

        if (startsWith(*it, "ip"))
            pattern = std::make_unique<IPAddressPattern>(value);
        else if (startsWith(*it, "host_regexp"))
            pattern = std::make_unique<HostRegexpPattern>(value);
        else if (startsWith(*it, "host"))
            pattern = std::make_unique<HostExactPattern>(value);
        else
            throw Exception("Unknown address pattern type: " + *it, ErrorCodes::UNKNOWN_ADDRESS_PATTERN_TYPE);

        patterns.emplace_back(std::move(pattern));
    }
}


User::User(const String & name_, const String & config_elem, Poco::Util::AbstractConfiguration & config)
    : name(name_)
{
    bool has_password = config.has(config_elem + ".password");
    bool has_password_sha256_hex = config.has(config_elem + ".password_sha256_hex");

    if (has_password && has_password_sha256_hex)
        throw Exception("Both fields 'password' and 'password_sha256_hex' are specified for user " + name + ". Must be only one of them.", ErrorCodes::BAD_ARGUMENTS);

    if (!has_password && !has_password_sha256_hex)
        throw Exception("Either 'password' or 'password_sha256_hex' must be specified for user " + name + ".", ErrorCodes::BAD_ARGUMENTS);

    if (has_password)
        password     = config.getString(config_elem + ".password");

    if (has_password_sha256_hex)
    {
        password_sha256_hex = Poco::toLower(config.getString(config_elem + ".password_sha256_hex"));

        if (password_sha256_hex.size() != 64)
            throw Exception("password_sha256_hex for user " + name + " has length " + toString(password_sha256_hex.size()) + " but must be exactly 64 symbols.", ErrorCodes::BAD_ARGUMENTS);
    }

    profile = config.getString(config_elem + ".profile");
    quota = config.getString(config_elem + ".quota");

    addresses.addFromConfig(config_elem + ".networks", config);

    /// Fill list of allowed databases.
    const auto config_sub_elem = config_elem + ".allow_databases";
    if (config.has(config_sub_elem))
    {
        Poco::Util::AbstractConfiguration::Keys config_keys;
        config.keys(config_sub_elem, config_keys);

        databases.reserve(config_keys.size());
        for (const auto & key : config_keys)
        {
            const auto database_name = config.getString(config_sub_elem + "." + key);
            databases.insert(database_name);
        }
    }
}


void Users::loadFromConfig(Poco::Util::AbstractConfiguration & config)
{
    Container new_cont;

    Poco::Util::AbstractConfiguration::Keys config_keys;
    config.keys("users", config_keys);

    for (const std::string & key : config_keys)
        new_cont.emplace(std::piecewise_construct, std::forward_as_tuple(key), std::forward_as_tuple(key, "users." + key, config));

    cont = std::move(new_cont);
}

const User & Users::get(const String & user_name, const String & password, const Poco::Net::IPAddress & address) const
{
    auto it = cont.find(user_name);

    if (cont.end() == it)
        throw Exception("Unknown user " + user_name, ErrorCodes::UNKNOWN_USER);

    if (!it->second.addresses.contains(address))
        throw Exception("User " + user_name + " is not allowed to connect from address " + address.toString(), ErrorCodes::IP_ADDRESS_NOT_ALLOWED);

    auto on_wrong_password = [&]()
    {
        if (password.empty())
            throw Exception("Password required for user " + user_name, ErrorCodes::REQUIRED_PASSWORD);
        else
            throw Exception("Wrong password for user " + user_name, ErrorCodes::WRONG_PASSWORD);
    };

    if (!it->second.password_sha256_hex.empty())
    {
        unsigned char hash[32];

        SHA256_CTX ctx;
        SHA256_Init(&ctx);
        SHA256_Update(&ctx, reinterpret_cast<const unsigned char *>(password.data()), password.size());
        SHA256_Final(hash, &ctx);

        String hash_hex;
        {
            WriteBufferFromString buf(hash_hex);
            HexWriteBuffer hex_buf(buf);
            hex_buf.write(reinterpret_cast<const char *>(hash), sizeof(hash));
        }

        Poco::toLowerInPlace(hash_hex);

        if (hash_hex != it->second.password_sha256_hex)
            on_wrong_password();
    }
    else if (password != it->second.password)
    {
        on_wrong_password();
    }

    return it->second;
}


const User & Users::get(const String & user_name)
{
    auto it = cont.find(user_name);

    if (cont.end() == it)
        throw Exception("Unknown user " + user_name, ErrorCodes::UNKNOWN_USER);

    return it->second;
}


bool Users::isAllowedDatabase(const std::string & user_name, const std::string & database_name) const
{
    auto it = cont.find(user_name);
    if (it == cont.end())
        throw Exception("Unknown user " + user_name, ErrorCodes::UNKNOWN_USER);

    const auto & user = it->second;
    return user.databases.empty() || user.databases.count(database_name);
}


}
