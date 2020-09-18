#pragma once

#if !defined(ARCADIA_BUILD)
#   include "config_core.h"
#endif

#include <Core/Types.h>

#if USE_LDAP
#   include <ldap.h>
#   define MAYBE_NORETURN
#else
#   define MAYBE_NORETURN [[noreturn]]
#endif

#include <chrono>


namespace DB
{

class LDAPClient
{
public:
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

        String auth_dn_prefix;
        String auth_dn_suffix;

        String user;
        String password;

        std::chrono::seconds operation_timeout{40};
        std::chrono::seconds network_timeout{30};
        std::chrono::seconds search_timeout{20};
        std::uint32_t search_limit = 100;
    };

    explicit LDAPClient(const Params & params_);
    ~LDAPClient();

    LDAPClient(const LDAPClient &) = delete;
    LDAPClient(LDAPClient &&) = delete;
    LDAPClient & operator= (const LDAPClient &) = delete;
    LDAPClient & operator= (LDAPClient &&) = delete;

protected:
    MAYBE_NORETURN void diag(const int rc);
    MAYBE_NORETURN void openConnection();
    int openConnection(const bool graceful_bind_failure = false);
    void closeConnection() noexcept;

protected:
    const Params params;
#if USE_LDAP
    LDAP * handle = nullptr;
#endif
};

class LDAPSimpleAuthClient
    : private LDAPClient
{
public:
    using LDAPClient::LDAPClient;
    bool check();
};

}

#undef MAYBE_NORETURN
