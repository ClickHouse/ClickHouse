#include <Storages/Kafka/Clients/Protocol/Messages/Sasl.h>

namespace Kafka::Protocol
{

void SaslHandshakeRequest::encode(Writer & w, Int16 version) const
{
    checkVersionSupported(version, min_version, max_version, "SaslHandshake");
    w.writeString(mechanism);
}

SaslHandshakeRequest SaslHandshakeRequest::decode(Reader & r, Int16 version)
{
    checkVersionSupported(version, min_version, max_version, "SaslHandshake");
    SaslHandshakeRequest req;
    req.mechanism = r.readString();
    return req;
}

void SaslHandshakeResponse::encode(Writer & w, Int16 version) const
{
    checkVersionSupported(version, SaslHandshakeRequest::min_version, SaslHandshakeRequest::max_version, "SaslHandshake");
    w.writeInt16(error_code);
    w.writeArray(mechanisms, [](Writer & wr, const String & mechanism) { wr.writeString(mechanism); });
}

SaslHandshakeResponse SaslHandshakeResponse::decode(Reader & r, Int16 version)
{
    checkVersionSupported(version, SaslHandshakeRequest::min_version, SaslHandshakeRequest::max_version, "SaslHandshake");
    SaslHandshakeResponse resp;
    resp.error_code = r.readInt16();
    resp.mechanisms = r.readArray<String>([](Reader & rd) { return rd.readString(); });
    return resp;
}

void SaslAuthenticateRequest::encode(Writer & w, Int16 version) const
{
    checkVersionSupported(version, min_version, max_version, "SaslAuthenticate");
    const bool flex = flexible(version);
    w.writeBytesField(auth_bytes, flex);
    w.writeTags(flex);
}

SaslAuthenticateRequest SaslAuthenticateRequest::decode(Reader & r, Int16 version)
{
    checkVersionSupported(version, min_version, max_version, "SaslAuthenticate");
    const bool flex = flexible(version);
    SaslAuthenticateRequest req;
    req.auth_bytes = r.readBytesField(flex);
    r.readTags(flex);
    return req;
}

void SaslAuthenticateResponse::encode(Writer & w, Int16 version) const
{
    checkVersionSupported(version, SaslAuthenticateRequest::min_version, SaslAuthenticateRequest::max_version, "SaslAuthenticate");
    const bool flex = SaslAuthenticateRequest::flexible(version);
    w.writeInt16(error_code);
    w.writeNullableStr(error_message, flex);
    w.writeBytesField(auth_bytes, flex);
    w.writeInt64(session_lifetime_ms);
    w.writeTags(flex);
}

SaslAuthenticateResponse SaslAuthenticateResponse::decode(Reader & r, Int16 version)
{
    checkVersionSupported(version, SaslAuthenticateRequest::min_version, SaslAuthenticateRequest::max_version, "SaslAuthenticate");
    const bool flex = SaslAuthenticateRequest::flexible(version);
    SaslAuthenticateResponse resp;
    resp.error_code = r.readInt16();
    resp.error_message = r.readNullableStr(flex);
    resp.auth_bytes = r.readBytesField(flex);
    resp.session_lifetime_ms = r.readInt64();
    r.readTags(flex);
    return resp;
}

}
