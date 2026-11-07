#pragma once

/// ApiVersions (api_key 18), supported range: v3.
///
/// Special case: even though the API is flexible, the response uses response
/// header v0 — the broker has to reply before it knows the client version.

#include <Storages/Kafka/Clients/Protocol/ApiKey.h>
#include <Storages/Kafka/Clients/Protocol/Wire.h>

#include <vector>

namespace Kafka::Protocol
{

struct ApiVersionRange
{
    Int16 api_key = 0;
    Int16 min_version = 0;
    Int16 max_version = 0;

    bool operator==(const ApiVersionRange &) const = default;
};

struct ApiVersionsResponse
{
    Int16 error_code = 0;
    std::vector<ApiVersionRange> api_keys;
    Int32 throttle_time_ms = 0;

    bool operator==(const ApiVersionsResponse &) const = default;

    static ApiVersionsResponse decode(Reader & r, Int16 version);
    void encode(Writer & w, Int16 version) const;
};

struct ApiVersionsRequest
{
    using Response = ApiVersionsResponse;
    static constexpr ApiKey api_key = ApiKey::ApiVersions;
    static constexpr Int16 min_version = 3;
    static constexpr Int16 max_version = 3;
    static constexpr bool flexible(Int16) { return true; }
    /// ApiVersions response always uses header v0, regardless of api_version.
    static constexpr bool responseHeaderFlexible(Int16) { return false; }

    String client_software_name;
    String client_software_version;

    bool operator==(const ApiVersionsRequest &) const = default;

    void encode(Writer & w, Int16 version) const;
    static ApiVersionsRequest decode(Reader & r, Int16 version);
};

}
