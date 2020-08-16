#pragma once

#include <Core/MySQL/IMySQLReadPacket.h>
#include <Core/MySQL/IMySQLWritePacket.h>

namespace DB
{

namespace MySQLProtocol
{

namespace Generic
{

const size_t MAX_PACKET_LENGTH = (1 << 24) - 1; // 16 mb

enum StatusFlags
{
    SERVER_SESSION_STATE_CHANGED = 0x4000
};

enum Capability
{
    CLIENT_CONNECT_WITH_DB = 0x00000008,
    CLIENT_PROTOCOL_41 = 0x00000200,
    CLIENT_SSL = 0x00000800,
    CLIENT_TRANSACTIONS = 0x00002000, // TODO
    CLIENT_SESSION_TRACK = 0x00800000, // TODO
    CLIENT_SECURE_CONNECTION = 0x00008000,
    CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA = 0x00200000,
    CLIENT_PLUGIN_AUTH = 0x00080000,
    CLIENT_DEPRECATE_EOF = 0x01000000,
};

class SSLRequest : public IMySQLReadPacket
{
public:
    uint32_t capability_flags;
    uint32_t max_packet_size;
    uint8_t character_set;

protected:
    void readPayloadImpl(ReadBuffer & buf) override;
};

class OKPacket : public IMySQLWritePacket, public IMySQLReadPacket
{
public:
    uint8_t header;
    uint32_t capabilities;
    uint64_t affected_rows;
    uint64_t last_insert_id;
    int16_t warnings = 0;
    uint32_t status_flags;
    String session_state_changes;
    String info;

protected:
    size_t getPayloadSize() const override;

    void readPayloadImpl(ReadBuffer & payload) override;

    void writePayloadImpl(WriteBuffer & buffer) const override;

public:
    OKPacket(uint32_t capabilities_);

    OKPacket(uint8_t header_, uint32_t capabilities_, uint64_t affected_rows_,
             uint32_t status_flags_, int16_t warnings_, String session_state_changes_ = "", String info_ = "");
};

class EOFPacket : public IMySQLWritePacket, public IMySQLReadPacket
{
public:
    UInt8 header = 0xfe;
    int warnings;
    int status_flags;

protected:
    size_t getPayloadSize() const override;

    void readPayloadImpl(ReadBuffer & payload) override;

    void writePayloadImpl(WriteBuffer & buffer) const override;

public:
    EOFPacket();

    EOFPacket(int warnings_, int status_flags_);
};

class ERRPacket : public IMySQLWritePacket, public IMySQLReadPacket
{
public:
    UInt8 header = 0xff;
    int error_code;
    String sql_state;
    String error_message;

protected:
    size_t getPayloadSize() const override;

    void readPayloadImpl(ReadBuffer & payload) override;

    void writePayloadImpl(WriteBuffer & buffer) const override;

public:
    ERRPacket();

    ERRPacket(int error_code_, String sql_state_, String error_message_);
};

class AuthSwitchPacket : public IMySQLReadPacket
{
public:
    String plugin_name;

    AuthSwitchPacket() = default;

protected:
    UInt8 header = 0x00;

    void readPayloadImpl(ReadBuffer & payload) override;
};

enum ResponsePacketType
{
    PACKET_OK = 0x00,
    PACKET_ERR = 0xff,
    PACKET_EOF = 0xfe,
    PACKET_AUTH_SWITCH = 0xfe,
    PACKET_LOCALINFILE = 0xfb,
};

/// https://dev.mysql.com/doc/internals/en/generic-response-packets.html
class ResponsePacket : public IMySQLReadPacket
{
public:
    OKPacket ok;
    ERRPacket err;
    EOFPacket eof;
    AuthSwitchPacket auth_switch;
    UInt64 column_length = 0;

    ResponsePacketType getType() { return packetType; }
protected:
    bool is_handshake = false;
    ResponsePacketType packetType = PACKET_OK;

    void readPayloadImpl(ReadBuffer & payload) override;

public:
    ResponsePacket(UInt32 server_capability_flags_);

    ResponsePacket(UInt32 server_capability_flags_, bool is_handshake_);
};

class LengthEncodedNumber : public IMySQLWritePacket
{
protected:
    uint64_t value;

    size_t getPayloadSize() const override;

    void writePayloadImpl(WriteBuffer & buffer) const override;

public:
    explicit LengthEncodedNumber(uint64_t value_);
};

}

}

}
