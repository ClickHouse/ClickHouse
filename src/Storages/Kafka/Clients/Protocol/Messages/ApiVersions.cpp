#include <Storages/Kafka/Clients/Protocol/Messages/ApiVersions.h>

namespace Kafka::Protocol
{

void ApiVersionsRequest::encode(Writer & w, Int16 version) const
{
    checkVersionSupported(version, min_version, max_version, "ApiVersions");
    w.writeCompactString(client_software_name);
    w.writeCompactString(client_software_version);
    w.writeEmptyTaggedFields();
}

ApiVersionsRequest ApiVersionsRequest::decode(Reader & r, Int16 version)
{
    checkVersionSupported(version, min_version, max_version, "ApiVersions");
    ApiVersionsRequest req;
    req.client_software_name = r.readCompactString();
    req.client_software_version = r.readCompactString();
    r.skipTaggedFields();
    return req;
}

void ApiVersionsResponse::encode(Writer & w, Int16 version) const
{
    checkVersionSupported(version, ApiVersionsRequest::min_version, ApiVersionsRequest::max_version, "ApiVersions");
    w.writeInt16(error_code);
    w.writeCompactArray(api_keys, [](Writer & wr, const ApiVersionRange & range)
    {
        wr.writeInt16(range.api_key);
        wr.writeInt16(range.min_version);
        wr.writeInt16(range.max_version);
        wr.writeEmptyTaggedFields();
    });
    w.writeInt32(throttle_time_ms);
    w.writeEmptyTaggedFields();
}

ApiVersionsResponse ApiVersionsResponse::decode(Reader & r, Int16 version)
{
    checkVersionSupported(version, ApiVersionsRequest::min_version, ApiVersionsRequest::max_version, "ApiVersions");
    ApiVersionsResponse resp;
    resp.error_code = r.readInt16();
    resp.api_keys = r.readCompactArray<ApiVersionRange>([](Reader & rd)
    {
        ApiVersionRange range;
        range.api_key = rd.readInt16();
        range.min_version = rd.readInt16();
        range.max_version = rd.readInt16();
        rd.skipTaggedFields();
        return range;
    });
    resp.throttle_time_ms = r.readInt32();
    r.skipTaggedFields();
    return resp;
}

}
