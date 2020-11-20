#pragma once

#include <common/types.h>

#include <chrono>
#include <set>
#include <vector>


namespace DB
{

struct LDAPRoleMappingRules
{
    String match = ".+";
    String replace = "$&";

    bool continue_on_match = false;
};

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
    String search_filter;
    String attribute = "cn";
    Scope scope = Scope::SUBTREE;

    bool fail_if_all_rules_mismatch = false;
    std::vector<LDAPRoleMappingRules> role_mapping_rules;
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

    std::chrono::seconds operation_timeout{40};
    std::chrono::seconds network_timeout{30};
    std::chrono::seconds search_timeout{20};
    std::uint32_t search_limit = 100;
};

}
