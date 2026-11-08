#include <Storages/Kafka/Clients/Protocol/RequestHeader.h>

namespace Kafka::Protocol
{

void RequestHeader::encode(Writer & w) const
{
    w.writeInt16(api_key);
    w.writeInt16(api_version);
    w.writeInt32(correlation_id);
    /// KIP-482: in flexible request header v2 the body becomes tagged-fields
    /// aware, but client_id itself stays a regular i16-length NULLABLE_STRING
    /// (`flexibleVersions: "none"` on the field in RequestHeaderData.json).
    /// Encoding it as a compact string is the most common first-contact bug:
    /// the broker silently drops the connection and every request times out.
    w.writeNullableString(client_id);
    if (flexible)
        w.writeEmptyTaggedFields();
}

RequestHeader RequestHeader::decode(Reader & r, bool flexible)
{
    RequestHeader h;
    h.api_key = r.readInt16();
    h.api_version = r.readInt16();
    h.correlation_id = r.readInt32();
    /// See encode(): client_id is non-compact even in v2.
    h.client_id = r.readNullableString();
    h.flexible = flexible;
    if (flexible)
        r.skipTaggedFields();
    return h;
}

void ResponseHeader::encode(Writer & w, bool flexible) const
{
    w.writeInt32(correlation_id);
    if (flexible)
        w.writeEmptyTaggedFields();
}

ResponseHeader ResponseHeader::decode(Reader & r, bool flexible)
{
    ResponseHeader h;
    h.correlation_id = r.readInt32();
    if (flexible)
        r.skipTaggedFields();
    return h;
}

}
