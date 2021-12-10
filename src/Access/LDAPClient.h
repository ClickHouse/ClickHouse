#pragma once

#if !defined(ARCADIA_BUILD)
#   include "config_core.h"
#endif

#include <common/types.h>

#if USE_LDAP
#   include <ldap.h>
#   define MAYBE_NORETURN
#else
#   define MAYBE_NORETURN [[noreturn]]
#endif

#include <chrono>
#include <optional>
#include <set>
#include <vector>


namespace DB
{

class LDAPClient
{
public:
    struct SearchParams
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

        void combineHash(std::size_t & seed) const;
    };

    struct RoleSearchParams
        : public SearchParams
    {
        String prefix;

        void combineHash(std::size_t & seed) const;
    };

    using RoleSearchParamsList = std::vector<RoleSearchParams>;

    using SearchResults = std::set<String>;
    using SearchResultsList = std::vector<SearchResults>;

    struct Params
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

        std::optional<SearchParams> user_dn_detection;

        std::chrono::seconds verification_cooldown{0};

        std::chrono::seconds operation_timeout{40};
        std::chrono::seconds network_timeout{30};
        std::chrono::seconds search_timeout{20};
        std::uint32_t search_limit = 100;

        void combineCoreHash(std::size_t & seed) const;
    };

    explicit LDAPClient(const Params & params_);
    ~LDAPClient();

    LDAPClient(const LDAPClient &) = delete;
    LDAPClient(LDAPClient &&) = delete;
    LDAPClient & operator= (const LDAPClient &) = delete;
    LDAPClient & operator= (LDAPClient &&) = delete;

protected:
    MAYBE_NORETURN void diag(const int rc, String text = "");
    MAYBE_NORETURN void openConnection();
    void closeConnection() noexcept;
    SearchResults search(const SearchParams & search_params);

protected:
    const Params params;
#if USE_LDAP
    LDAP * handle = nullptr;
#endif
    String final_user_name;
    String final_bind_dn;
    String final_user_dn;
};

class LDAPSimpleAuthClient
    : private LDAPClient
{
public:
    using LDAPClient::LDAPClient;
    bool authenticate(const RoleSearchParamsList * role_search_params, SearchResultsList * role_search_results);
};

}

#undef MAYBE_NORETURN
