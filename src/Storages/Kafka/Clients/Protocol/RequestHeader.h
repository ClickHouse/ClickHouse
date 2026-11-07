#pragma once

/// Request/response headers and the i32 size-prefix framing used on the wire.
///
/// Which header version an API uses depends on (api_key, api_version):
///   * Non-flexible API -> request header v1, response header v0.
///   * Flexible API     -> request header v2, response header v1.
///
/// Special case: the response to ApiVersions uses header v0 regardless of
/// api_version — the broker replies before it knows which version the client
/// speaks, so the header cannot grow tagged fields.

#include <Storages/Kafka/Clients/Protocol/Wire.h>

#include <optional>

namespace Kafka::Protocol
{

struct RequestHeader
{
    Int16 api_key = 0;
    Int16 api_version = 0;
    Int32 correlation_id = 0;
    std::optional<String> client_id;
    /// true -> header v2 (tagged fields), false -> header v1.
    bool flexible = true;

    bool operator==(const RequestHeader &) const = default;

    void encode(Writer & w) const;
    static RequestHeader decode(Reader & r, bool flexible);
};

struct ResponseHeader
{
    Int32 correlation_id = 0;

    void encode(Writer & w, bool flexible) const;
    static ResponseHeader decode(Reader & r, bool flexible);
};

/// Build a complete wire frame: i32 size prefix back-filled around whatever
/// `fill` writes (header + body).
template <typename F>
String buildFrame(F && fill)
{
    Writer w;
    w.writeInt32(0); /// placeholder
    fill(w);
    const size_t body_len = w.size() - 4;
    if (body_len > static_cast<size_t>(INT32_MAX))
        throwProtocol(fmt::format("Frame size exceeds i32: {}", body_len));
    w.overwriteInt32At(0, static_cast<Int32>(body_len));
    return w.take();
}

}
