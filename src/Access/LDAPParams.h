#pragma once

#include <Core/Types.h>

#include <chrono>


namespace DB
{

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

    enum class TLSCertVerify
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
    TLSCertVerify tls_cert_verify = TLSCertVerify::DEMAND;
    String ca_cert_dir;
    String ca_cert_file;

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

}
