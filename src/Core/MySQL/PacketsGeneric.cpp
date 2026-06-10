#include <Core/MySQL/PacketsGeneric.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

namespace DB
{

namespace MySQLProtocol
{

namespace Generic
{

static const size_t MYSQL_ERRMSG_SIZE = 512;

void SSLRequest::readPayloadImpl(ReadBuffer & buf)
{
    buf.readStrict(reinterpret_cast<char *>(&capability_flags), 4);
    buf.readStrict(reinterpret_cast<char *>(&max_packet_size), 4);
    buf.readStrict(reinterpret_cast<char *>(&character_set), 1);
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

}
