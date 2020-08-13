#include <Core/MySQL/MySQLPackets.h>
#include <Core/MySQLProtocol.h>

namespace DB
{

namespace MySQLProtocol
{

void SSLRequest::readPayloadImpl(ReadBuffer & buf)
{
    buf.readStrict(reinterpret_cast<char *>(&capability_flags), 4);
    buf.readStrict(reinterpret_cast<char *>(&max_packet_size), 4);
    buf.readStrict(reinterpret_cast<char *>(&character_set), 1);
}

Handshake::Handshake() : connection_id(0x00), capability_flags(0x00), character_set(0x00), status_flags(0x00)
{
}

Handshake::Handshake(
    uint32_t capability_flags_, uint32_t connection_id_, String server_version_, String auth_plugin_name_, String auth_plugin_data_)
    : protocol_version(0xa), server_version(std::move(server_version_)), connection_id(connection_id_), capability_flags(capability_flags_),
      character_set(CharacterSet::utf8_general_ci), status_flags(0), auth_plugin_name(std::move(auth_plugin_name_)),
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
    if (capability_flags & MySQLProtocol::CLIENT_PLUGIN_AUTH)
    {
        payload.readStrict(reinterpret_cast<char *>(&auth_plugin_data_length), 1);
    }
    else
    {
        payload.ignore(1);
    }

    payload.ignore(10);
    if (capability_flags & MySQLProtocol::CLIENT_SECURE_CONNECTION)
    {
        UInt8 part2_length = (SCRAMBLE_LENGTH - AUTH_PLUGIN_DATA_PART_1_LENGTH);
        auth_plugin_data.resize(SCRAMBLE_LENGTH);
        payload.readStrict(auth_plugin_data.data() + AUTH_PLUGIN_DATA_PART_1_LENGTH, part2_length);
        payload.ignore(1);
    }

    if (capability_flags & MySQLProtocol::CLIENT_PLUGIN_AUTH)
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
    : capability_flags(capability_flags_), max_packet_size(max_packet_size_), character_set(character_set_), username(std::move(username_)),
      database(std::move(database_)), auth_response(std::move(auth_response_)), auth_plugin_name(std::move(auth_plugin_name_))
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

OKPacket::OKPacket(uint32_t capabilities_)
    : header(0x00), capabilities(capabilities_), affected_rows(0x00), last_insert_id(0x00), status_flags(0x00)
{
}

OKPacket::OKPacket(
    uint8_t header_, uint32_t capabilities_, uint64_t affected_rows_, uint32_t status_flags_, int16_t warnings_,
    String session_state_changes_, String info_)
    : header(header_), capabilities(capabilities_), affected_rows(affected_rows_), last_insert_id(0), warnings(warnings_),
      status_flags(status_flags_), session_state_changes(std::move(session_state_changes_)), info(std::move(info_))
{
}

size_t OKPacket::getPayloadSize() const
{
    size_t result = 2 + getLengthEncodedNumberSize(affected_rows);

    if (capabilities & CLIENT_PROTOCOL_41)
    {
        result += 4;
    }
    else if (capabilities & CLIENT_TRANSACTIONS)
    {
        result += 2;
    }

    if (capabilities & CLIENT_SESSION_TRACK)
    {
        result += getLengthEncodedStringSize(info);
        if (status_flags & SERVER_SESSION_STATE_CHANGED)
            result += getLengthEncodedStringSize(session_state_changes);
    }
    else
    {
        result += info.size();
    }

    return result;
}

void OKPacket::readPayloadImpl(ReadBuffer & payload)

{
    payload.readStrict(reinterpret_cast<char *>(&header), 1);
    affected_rows = readLengthEncodedNumber(payload);
    last_insert_id = readLengthEncodedNumber(payload);

    if (capabilities & CLIENT_PROTOCOL_41)
    {
        payload.readStrict(reinterpret_cast<char *>(&status_flags), 2);
        payload.readStrict(reinterpret_cast<char *>(&warnings), 2);
    }
    else if (capabilities & CLIENT_TRANSACTIONS)
    {
        payload.readStrict(reinterpret_cast<char *>(&status_flags), 2);
    }

    if (capabilities & CLIENT_SESSION_TRACK)
    {
        readLengthEncodedString(info, payload);
        if (status_flags & SERVER_SESSION_STATE_CHANGED)
        {
            readLengthEncodedString(session_state_changes, payload);
        }
    }
    else
    {
        readString(info, payload);
    }
}

void OKPacket::writePayloadImpl(WriteBuffer & buffer) const

{
    buffer.write(header);
    writeLengthEncodedNumber(affected_rows, buffer);
    writeLengthEncodedNumber(last_insert_id, buffer); /// last insert-id

    if (capabilities & CLIENT_PROTOCOL_41)
    {
        buffer.write(reinterpret_cast<const char *>(&status_flags), 2);
        buffer.write(reinterpret_cast<const char *>(&warnings), 2);
    }
    else if (capabilities & CLIENT_TRANSACTIONS)
    {
        buffer.write(reinterpret_cast<const char *>(&status_flags), 2);
    }

    if (capabilities & CLIENT_SESSION_TRACK)
    {
        writeLengthEncodedString(info, buffer);
        if (status_flags & SERVER_SESSION_STATE_CHANGED)
            writeLengthEncodedString(session_state_changes, buffer);
    }
    else
    {
        writeString(info, buffer);
    }
}

EOFPacket::EOFPacket() : warnings(0x00), status_flags(0x00)
{
}

EOFPacket::EOFPacket(int warnings_, int status_flags_)
    : warnings(warnings_), status_flags(status_flags_)
{
}

size_t EOFPacket::getPayloadSize() const
{
    return 5;
}

void EOFPacket::readPayloadImpl(ReadBuffer & payload)
{
    payload.readStrict(reinterpret_cast<char *>(&header), 1);
    assert(header == 0xfe);
    payload.readStrict(reinterpret_cast<char *>(&warnings), 2);
    payload.readStrict(reinterpret_cast<char *>(&status_flags), 2);
}

void EOFPacket::writePayloadImpl(WriteBuffer & buffer) const
{
    buffer.write(header); // EOF header
    buffer.write(reinterpret_cast<const char *>(&warnings), 2);
    buffer.write(reinterpret_cast<const char *>(&status_flags), 2);
}

void AuthSwitchPacket::readPayloadImpl(ReadBuffer & payload)
{
    payload.readStrict(reinterpret_cast<char *>(&header), 1);
    assert(header == 0xfe);
    readStringUntilEOF(plugin_name, payload);
}

ERRPacket::ERRPacket() : error_code(0x00)
{
}

ERRPacket::ERRPacket(int error_code_, String sql_state_, String error_message_)
    : error_code(error_code_), sql_state(std::move(sql_state_)), error_message(std::move(error_message_))
{
}

size_t ERRPacket::getPayloadSize() const
{
    return 4 + sql_state.length() + std::min(error_message.length(), MYSQL_ERRMSG_SIZE);
}

void ERRPacket::readPayloadImpl(ReadBuffer & payload)
{
    payload.readStrict(reinterpret_cast<char *>(&header), 1);
    assert(header == 0xff);

    payload.readStrict(reinterpret_cast<char *>(&error_code), 2);

    /// SQL State [optional: # + 5bytes string]
    UInt8 sharp = static_cast<unsigned char>(*payload.position());
    if (sharp == 0x23)
    {
        payload.ignore(1);
        sql_state.resize(5);
        payload.readStrict(reinterpret_cast<char *>(sql_state.data()), 5);
    }
    readString(error_message, payload);
}

void ERRPacket::writePayloadImpl(WriteBuffer & buffer) const
{
    buffer.write(header);
    buffer.write(reinterpret_cast<const char *>(&error_code), 2);
    buffer.write('#');
    buffer.write(sql_state.data(), sql_state.length());
    buffer.write(error_message.data(), std::min(error_message.length(), MYSQL_ERRMSG_SIZE));
}

ResponsePacket::ResponsePacket(UInt32 server_capability_flags_)
    : ok(OKPacket(server_capability_flags_))
{
}

ResponsePacket::ResponsePacket(UInt32 server_capability_flags_, bool is_handshake_)
    : ok(OKPacket(server_capability_flags_)), is_handshake(is_handshake_)
{
}

void ResponsePacket::readPayloadImpl(ReadBuffer & payload)
{
    UInt16 header = static_cast<unsigned char>(*payload.position());
    switch (header)
    {
        case PACKET_OK:
            packetType = PACKET_OK;
            ok.readPayloadWithUnpacked(payload);
            break;
        case PACKET_ERR:
            packetType = PACKET_ERR;
            err.readPayloadWithUnpacked(payload);
            break;
        case PACKET_EOF:
            if (is_handshake)
            {
                packetType = PACKET_AUTH_SWITCH;
                auth_switch.readPayloadWithUnpacked(payload);
            }
            else
            {
                packetType = PACKET_EOF;
                eof.readPayloadWithUnpacked(payload);
            }
            break;
        case PACKET_LOCALINFILE:
            packetType = PACKET_LOCALINFILE;
            break;
        default:
            packetType = PACKET_OK;
            column_length = readLengthEncodedNumber(payload);
    }
}

ColumnDefinitionPacket::ColumnDefinitionPacket()
    : character_set(0x00), column_length(0), column_type(MYSQL_TYPE_DECIMAL), flags(0x00)
{
}

ColumnDefinitionPacket::ColumnDefinitionPacket(
    String schema_, String table_, String org_table_, String name_, String org_name_, uint16_t character_set_, uint32_t column_length_,
    ColumnType column_type_, uint16_t flags_, uint8_t decimals_)
    : schema(std::move(schema_)), table(std::move(table_)), org_table(std::move(org_table_)), name(std::move(name_)),
      org_name(std::move(org_name_)), character_set(character_set_), column_length(column_length_), column_type(column_type_),
      flags(flags_), decimals(decimals_)
{
}

ColumnDefinitionPacket::ColumnDefinitionPacket(
    String name_, uint16_t character_set_, uint32_t column_length_, ColumnType column_type_, uint16_t flags_, uint8_t decimals_)
    : ColumnDefinitionPacket("", "", "", std::move(name_), "", character_set_, column_length_, column_type_, flags_, decimals_)
{
}

size_t ColumnDefinitionPacket::getPayloadSize() const
{
    return 13 + getLengthEncodedStringSize("def") + getLengthEncodedStringSize(schema) + getLengthEncodedStringSize(table) + getLengthEncodedStringSize(org_table) + \
            getLengthEncodedStringSize(name) + getLengthEncodedStringSize(org_name) + getLengthEncodedNumberSize(next_length);
}

void ColumnDefinitionPacket::readPayloadImpl(ReadBuffer & payload)
{
    String def;
    readLengthEncodedString(def, payload);
    assert(def == "def");
    readLengthEncodedString(schema, payload);
    readLengthEncodedString(table, payload);
    readLengthEncodedString(org_table, payload);
    readLengthEncodedString(name, payload);
    readLengthEncodedString(org_name, payload);
    next_length = readLengthEncodedNumber(payload);
    payload.readStrict(reinterpret_cast<char *>(&character_set), 2);
    payload.readStrict(reinterpret_cast<char *>(&column_length), 4);
    payload.readStrict(reinterpret_cast<char *>(&column_type), 1);
    payload.readStrict(reinterpret_cast<char *>(&flags), 2);
    payload.readStrict(reinterpret_cast<char *>(&decimals), 2);
    payload.ignore(2);
}

void ColumnDefinitionPacket::writePayloadImpl(WriteBuffer & buffer) const
{
    writeLengthEncodedString(std::string("def"), buffer); /// always "def"
    writeLengthEncodedString(schema, buffer);
    writeLengthEncodedString(table, buffer);
    writeLengthEncodedString(org_table, buffer);
    writeLengthEncodedString(name, buffer);
    writeLengthEncodedString(org_name, buffer);
    writeLengthEncodedNumber(next_length, buffer);
    buffer.write(reinterpret_cast<const char *>(&character_set), 2);
    buffer.write(reinterpret_cast<const char *>(&column_length), 4);
    buffer.write(reinterpret_cast<const char *>(&column_type), 1);
    buffer.write(reinterpret_cast<const char *>(&flags), 2);
    buffer.write(reinterpret_cast<const char *>(&decimals), 2);
    writeChar(0x0, 2, buffer);
}

void ComFieldList::readPayloadImpl(ReadBuffer & payload)
{
    // Command byte has been already read from payload.
    readNullTerminated(table, payload);
    readStringUntilEOF(field_wildcard, payload);
}

LengthEncodedNumber::LengthEncodedNumber(uint64_t value_) : value(value_)
{
}

size_t LengthEncodedNumber::getPayloadSize() const
{
    return getLengthEncodedNumberSize(value);
}

void LengthEncodedNumber::writePayloadImpl(WriteBuffer & buffer) const
{
    writeLengthEncodedNumber(value, buffer);
}

}

}
