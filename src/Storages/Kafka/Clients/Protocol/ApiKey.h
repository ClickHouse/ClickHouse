#pragma once

/// API keys for the subset of the Kafka protocol this client implements.

#include <base/types.h>

namespace Kafka::Protocol
{

/// The base type is the wire type (INT16 in the request header) on purpose.
enum class ApiKey : Int16 // NOLINT(performance-enum-size)
{
    Produce = 0,
    Fetch = 1,
    ListOffsets = 2,
    Metadata = 3,
    OffsetCommit = 8,
    OffsetFetch = 9,
    FindCoordinator = 10,
    SaslHandshake = 17,
    ApiVersions = 18,
    SaslAuthenticate = 36,
};

constexpr Int16 toWire(ApiKey key)
{
    return static_cast<Int16>(key);
}

constexpr const char * apiKeyName(ApiKey key)
{
    switch (key)
    {
        case ApiKey::Produce: return "Produce";
        case ApiKey::Fetch: return "Fetch";
        case ApiKey::ListOffsets: return "ListOffsets";
        case ApiKey::Metadata: return "Metadata";
        case ApiKey::OffsetCommit: return "OffsetCommit";
        case ApiKey::OffsetFetch: return "OffsetFetch";
        case ApiKey::FindCoordinator: return "FindCoordinator";
        case ApiKey::SaslHandshake: return "SaslHandshake";
        case ApiKey::ApiVersions: return "ApiVersions";
        case ApiKey::SaslAuthenticate: return "SaslAuthenticate";
    }
    return "UNKNOWN";
}

}
