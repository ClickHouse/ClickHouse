#pragma once

/// SaslHandshake (api_key 17) v1 — NON-flexible (classic strings, no tagged
/// fields; this API never went flexible).
/// SaslAuthenticate (api_key 36) v1-v2 — v2 is flexible, v1 (Kafka 2.4 max)
/// is the non-flexible form with the same fields.

#include <Storages/Kafka/Clients/Protocol/ApiKey.h>
#include <Storages/Kafka/Clients/Protocol/Wire.h>

#include <optional>
#include <vector>

namespace Kafka::Protocol
{

struct SaslHandshakeResponse
{
    Int16 error_code = 0;
    std::vector<String> mechanisms;

    bool operator==(const SaslHandshakeResponse &) const = default;

    static SaslHandshakeResponse decode(Reader & r, Int16 version);
    void encode(Writer & w, Int16 version) const;
};

struct SaslHandshakeRequest
{
    using Response = SaslHandshakeResponse;
    static constexpr ApiKey api_key = ApiKey::SaslHandshake;
    static constexpr Int16 min_version = 1;
    static constexpr Int16 max_version = 1;
    static constexpr bool flexible(Int16) { return false; }
    static constexpr bool responseHeaderFlexible(Int16) { return false; }

    String mechanism;

    bool operator==(const SaslHandshakeRequest &) const = default;

    void encode(Writer & w, Int16 version) const;
    static SaslHandshakeRequest decode(Reader & r, Int16 version);
};

struct SaslAuthenticateResponse
{
    Int16 error_code = 0;
    std::optional<String> error_message;
    Bytes auth_bytes;
    Int64 session_lifetime_ms = 0;

    bool operator==(const SaslAuthenticateResponse &) const = default;

    static SaslAuthenticateResponse decode(Reader & r, Int16 version);
    void encode(Writer & w, Int16 version) const;
};

struct SaslAuthenticateRequest
{
    using Response = SaslAuthenticateResponse;
    static constexpr ApiKey api_key = ApiKey::SaslAuthenticate;
    static constexpr Int16 min_version = 1;
    static constexpr Int16 max_version = 2;
    static constexpr bool flexible(Int16 version) { return version >= 2; }
    static constexpr bool responseHeaderFlexible(Int16 version) { return version >= 2; }

    Bytes auth_bytes;

    bool operator==(const SaslAuthenticateRequest &) const = default;

    void encode(Writer & w, Int16 version) const;
    static SaslAuthenticateRequest decode(Reader & r, Int16 version);
};

}
