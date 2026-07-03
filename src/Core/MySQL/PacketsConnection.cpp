#include <Core/MySQL/PacketsConnection.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Core/MySQL/PacketsGeneric.h>

namespace DB
{

namespace MySQLProtocol
{

using namespace Generic;

namespace ConnectionPhase
{

static const size_t SCRAMBLE_LENGTH = 20;
static const size_t AUTH_PLUGIN_DATA_PART_1_LENGTH = 8;

Handshake::Handshake() : connection_id(0x00), capability_flags(0x00), character_set(0x00), status_flags(0x00)
{
}

Handshake::Handshake(
    uint32_t capability_flags_, uint32_t connection_id_,
    String server_version_, String auth_plugin_name_, String auth_plugin_data_, uint8_t charset_)
    : protocol_version(0xa), server_version(std::move(server_version_)), connection_id(connection_id_), capability_flags(capability_flags_),
      character_set(charset_), status_flags(0), auth_plugin_name(std::move(auth_plugin_name_)),
      auth_plugin_data(std::move(auth_plugin_data_))
{
}

size_t Handshake::getPayloadSize() const
{
    return 26 + server_version.size() + auth_plugin_data.size() + auth_plugin_name.size();
}

void Handshake::readPayloadImpl(ReadBuffer & payload)
{
    payload.readStrict(reinterpret_cast<char *>(&protocol_version), 1);
    readNullTerminated(server_version, payload);
    payload.readStrict(reinterpret_cast<char *>(&connection_id), 4);

    auth_plugin_data.resize(AUTH_PLUGIN_DATA_PART_1_LENGTH);
    payload.readStrict(auth_plugin_data.data(), AUTH_PLUGIN_DATA_PART_1_LENGTH);

    payload.ignore(1);
    payload.readStrict(reinterpret_cast<char *>(&capability_flags), 2);
    payload.readStrict(reinterpret_cast<char *>(&character_set), 1);
    payload.readStrict(reinterpret_cast<char *>(&status_flags), 2);
    payload.readStrict((reinterpret_cast<char *>(&capability_flags)) + 2, 2);

    UInt8 auth_plugin_data_length = 0;
    if (capability_flags & Capability::CLIENT_PLUGIN_AUTH)
    {
        payload.readStrict(reinterpret_cast<char *>(&auth_plugin_data_length), 1);
    }
    else
    {
        payload.ignore(1);
    }

    payload.ignore(10);
    if (capability_flags & Capability::CLIENT_SECURE_CONNECTION)
    {
        UInt8 part2_length = (SCRAMBLE_LENGTH - AUTH_PLUGIN_DATA_PART_1_LENGTH);
        auth_plugin_data.resize(SCRAMBLE_LENGTH);
        payload.readStrict(auth_plugin_data.data() + AUTH_PLUGIN_DATA_PART_1_LENGTH, part2_length);
        payload.ignore(1);
    }

    if (capability_flags & Capability::CLIENT_PLUGIN_AUTH)
    {
        readNullTerminated(auth_plugin_name, payload);
    }
}

void Handshake::writePayloadImpl(WriteBuffer & buffer) const
{
    buffer.write(static_cast<char>(protocol_version));
    writeNulTerminatedString(server_version, buffer);
    buffer.write(reinterpret_cast<const char *>(&connection_id), 4);
    writeNulTerminatedString(auth_plugin_data.substr(0, AUTH_PLUGIN_DATA_PART_1_LENGTH), buffer);
    buffer.write(reinterpret_cast<const char *>(&capability_flags), 2);
    buffer.write(reinterpret_cast<const char *>(&character_set), 1);
    buffer.write(reinterpret_cast<const char *>(&status_flags), 2);
    buffer.write((reinterpret_cast<const char *>(&capability_flags)) + 2, 2);
    buffer.write(static_cast<char>(auth_plugin_data.size()));
    writeChar(0x0, 10, buffer);
    writeString(auth_plugin_data.substr(AUTH_PLUGIN_DATA_PART_1_LENGTH, auth_plugin_data.size() - AUTH_PLUGIN_DATA_PART_1_LENGTH), buffer);
    writeString(auth_plugin_name, buffer);
    writeChar(0x0, 1, buffer);
}

HandshakeResponse::HandshakeResponse() : capability_flags(0x00), max_packet_size(0x00), character_set(0x00)
{
}

HandshakeResponse::HandshakeResponse(
    UInt32 capability_flags_, UInt32 max_packet_size_, UInt8 character_set_, const String & username_, const String & database_,
    const String & auth_response_, const String & auth_plugin_name_)
    : capability_flags(capability_flags_), max_packet_size(max_packet_size_), character_set(character_set_), username(username_),
      database(database_), auth_response(auth_response_), auth_plugin_name(auth_plugin_name_)
{
}

size_t HandshakeResponse::getPayloadSize() const
{
    size_t size = 0;
    size += 4 + 4 + 1 + 23;
    size += username.size() + 1;

    if (capability_flags & CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA)
    {
        size += getLengthEncodedStringSize(auth_response);
    }
    else if (capability_flags & CLIENT_SECURE_CONNECTION)
    {
        size += (1 + auth_response.size());
    }
    else
    {
        size += (auth_response.size() + 1);
    }
    if (capability_flags & CLIENT_CONNECT_WITH_DB)
    {
        size += (database.size() + 1);
    }
    if (capability_flags & CLIENT_PLUGIN_AUTH)
    {
        size += (auth_plugin_name.size() + 1);
    }
    return size;
}

void HandshakeResponse::readPayloadImpl(ReadBuffer & payload)
{
    payload.readStrict(reinterpret_cast<char *>(&capability_flags), 4);
    payload.readStrict(reinterpret_cast<char *>(&max_packet_size), 4);
    payload.readStrict(reinterpret_cast<char *>(&character_set), 1);
    payload.ignore(23);

    readNullTerminated(username, payload);

    if (capability_flags & CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA)
    {
        readLengthEncodedString(auth_response, payload);
    }
    else if (capability_flags & CLIENT_SECURE_CONNECTION)
    {
        char len;
        payload.readStrict(len);
        auth_response.resize(static_cast<unsigned int>(len));
        payload.readStrict(auth_response.data(), len);
    }
    else
    {
        readNullTerminated(auth_response, payload);
    }

    if (capability_flags & CLIENT_CONNECT_WITH_DB)
    {
        readNullTerminated(database, payload);
    }

    if (capability_flags & CLIENT_PLUGIN_AUTH)
    {
        readNullTerminated(auth_plugin_name, payload);
    }
}

void HandshakeResponse::writePayloadImpl(WriteBuffer & buffer) const
{
    buffer.write(reinterpret_cast<const char *>(&capability_flags), 4);
    buffer.write(reinterpret_cast<const char *>(&max_packet_size), 4);
    buffer.write(reinterpret_cast<const char *>(&character_set), 1);
    writeChar(0x0, 23, buffer);

    writeNulTerminatedString(username, buffer);
    if (capability_flags & CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA)
    {
        writeLengthEncodedString(auth_response, buffer);
    }
    else if (capability_flags & CLIENT_SECURE_CONNECTION)
    {
        writeChar(auth_response.size(), buffer);
        writeString(auth_response.data(), auth_response.size(), buffer);
    }
    else
    {
        writeNulTerminatedString(auth_response, buffer);
    }

    if (capability_flags & CLIENT_CONNECT_WITH_DB)
    {
        writeNulTerminatedString(database, buffer);
    }

    if (capability_flags & CLIENT_PLUGIN_AUTH)
    {
        writeNulTerminatedString(auth_plugin_name, buffer);
    }
}

AuthSwitchRequest::AuthSwitchRequest(String plugin_name_, String auth_plugin_data_)
    : plugin_name(std::move(plugin_name_)), auth_plugin_data(std::move(auth_plugin_data_))
{
}

size_t AuthSwitchRequest::getPayloadSize() const
{
    return 2 + plugin_name.size() + auth_plugin_data.size();
}

void AuthSwitchRequest::writePayloadImpl(WriteBuffer & buffer) const
{
    buffer.write(0xfe);
    writeNulTerminatedString(plugin_name, buffer);
    writeString(auth_plugin_data, buffer);
}

void AuthSwitchResponse::readPayloadImpl(ReadBuffer & payload)
{
    readStringUntilEOF(value, payload);
}

size_t AuthMoreData::getPayloadSize() const
{
    return 1 + data.size();
}

void AuthMoreData::writePayloadImpl(WriteBuffer & buffer) const
{
    buffer.write(0x01);
    writeString(data, buffer);
}

}

}

}
