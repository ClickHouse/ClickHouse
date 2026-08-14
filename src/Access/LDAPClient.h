#pragma once

#include "config.h"

#include <base/types.h>

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

class SipHash;

namespace DB
{

class LDAPClient
{
public:
    struct SearchParams
    {
        enum class Scope : uint8_t
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

        void updateHash(SipHash & hash) const;
    };

    struct RoleSearchParams
        : public SearchParams
    {
        String prefix;

        void updateHash(SipHash & hash) const;
    };

    using RoleSearchParamsList = std::vector<RoleSearchParams>;

    using SearchResults = std::set<String>;
    using SearchResultsList = std::vector<SearchResults>;

    struct Params
    {
        enum class ProtocolVersion : uint8_t
        {
            V2,
            V3
        };

        enum class TLSEnable : uint8_t
        {
            NO,
            YES_STARTTLS,
            YES
        };

        enum class TLSProtocolVersion : uint8_t
        {
            SSL2,
            SSL3,
            TLS1_0,
            TLS1_1,
            TLS1_2
        };

        enum class TLSRequireCert : uint8_t
        {
            NEVER,
            ALLOW,
            TRY,
            DEMAND
        };

        enum class SASLMechanism : uint8_t
        {
            UNKNOWN,
            SIMPLE
        };

        ProtocolVersion protocol_version = ProtocolVersion::V3;

        String host;
        UInt16 port = 636;

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
        UInt32 search_limit = 256; /// An arbitrary number, no particular motivation for this value.

        void updateHash(SipHash & hash) const;
    };

    explicit LDAPClient(const Params & params_);
    ~LDAPClient();

    LDAPClient(const LDAPClient &) = delete;
    LDAPClient(LDAPClient &&) = delete;
    LDAPClient & operator= (const LDAPClient &) = delete;
    LDAPClient & operator= (LDAPClient &&) = delete;

protected:
    MAYBE_NORETURN void handleError(int result_code, String text = "");
    MAYBE_NORETURN bool openConnection();
    void closeConnection() noexcept;
    SearchResults search(const SearchParams & search_params);

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
