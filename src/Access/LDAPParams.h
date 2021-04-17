#pragma once

#include <common/types.h>

#include <boost/container_hash/hash.hpp>

#include <chrono>
#include <set>
#include <vector>


namespace DB
{

struct LDAPSearchParams
{
    enum class Scope
    {
        BASE,
        ONE_LEVEL,
        SUBTREE,
        CHILDREN
    };

    String base_dn;
    Scope scope = Scope::SUBTREE;
    String search_filter;
    String attribute = "cn";
    String prefix;

    void combineHash(std::size_t & seed) const
    {
        boost::hash_combine(seed, base_dn);
        boost::hash_combine(seed, static_cast<int>(scope));
        boost::hash_combine(seed, search_filter);
        boost::hash_combine(seed, attribute);
        boost::hash_combine(seed, prefix);
    }
};

using LDAPSearchParamsList = std::vector<LDAPSearchParams>;
using LDAPSearchResults = std::set<String>;
using LDAPSearchResultsList = std::vector<LDAPSearchResults>;

struct LDAPServerParams
{
    enum class ProtocolVersion
    {
        V2,
        V3
    };

    enum class TLSEnable
    {
        NO,
        YES_STARTTLS,
        YES
    };

    enum class TLSProtocolVersion
    {
        SSL2,
        SSL3,
        TLS1_0,
        TLS1_1,
        TLS1_2
    };

    enum class TLSRequireCert
    {
        NEVER,
        ALLOW,
        TRY,
        DEMAND
    };

    enum class SASLMechanism
    {
        UNKNOWN,
        SIMPLE
    };

    ProtocolVersion protocol_version = ProtocolVersion::V3;

    String host;
    std::uint16_t port = 636;

    TLSEnable enable_tls = TLSEnable::YES;
    TLSProtocolVersion tls_minimum_protocol_version = TLSProtocolVersion::TLS1_2;
    TLSRequireCert tls_require_cert = TLSRequireCert::DEMAND;
    String tls_cert_file;
    String tls_key_file;
    String tls_ca_cert_file;
    String tls_ca_cert_dir;
    String tls_cipher_suite;

    SASLMechanism sasl_mechanism = SASLMechanism::SIMPLE;

    String bind_dn;
    String user;
    String password;

    std::chrono::seconds verification_cooldown{0};

    std::chrono::seconds operation_timeout{40};
    std::chrono::seconds network_timeout{30};
    std::chrono::seconds search_timeout{20};
    std::uint32_t search_limit = 100;

    void combineCoreHash(std::size_t & seed) const
    {
        boost::hash_combine(seed, host);
        boost::hash_combine(seed, port);
        boost::hash_combine(seed, bind_dn);
        boost::hash_combine(seed, user);
        boost::hash_combine(seed, password);
    }
};

}
