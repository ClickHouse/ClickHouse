#pragma once

#include <Core/MySQL/IMySQLReadPacket.h>
#include <Core/MySQL/IMySQLWritePacket.h>
#include <IO/WriteHelpers.h>

namespace DB
{

namespace MySQLProtocol
{

class SSLRequest : public IMySQLReadPacket
{
public:
    uint32_t capability_flags;
    uint32_t max_packet_size;
    uint8_t character_set;

protected:
    void readPayloadImpl(ReadBuffer & buf) override;
};

class Handshake : public IMySQLWritePacket, public IMySQLReadPacket
{
public:
    int protocol_version = 0xa;
    String server_version;
    uint32_t connection_id;
    uint32_t capability_flags;
    uint8_t character_set;
    uint32_t status_flags;
    String auth_plugin_name;
    String auth_plugin_data;

protected:
    size_t getPayloadSize() const override;

    void readPayloadImpl(ReadBuffer & payload) override;

    void writePayloadImpl(WriteBuffer & buffer) const override;

public:
    Handshake();

    Handshake(uint32_t capability_flags_, uint32_t connection_id_, String server_version_, String auth_plugin_name_, String auth_plugin_data_);
};

class HandshakeResponse : public IMySQLWritePacket, public IMySQLReadPacket
{
public:
    uint32_t capability_flags;
    uint32_t max_packet_size;
    uint8_t character_set;
    String username;
    String database;
    String auth_response;
    String auth_plugin_name;

protected:
    size_t getPayloadSize() const override;

    void readPayloadImpl(ReadBuffer & payload) override;

    void writePayloadImpl(WriteBuffer & buffer) const override;

public:
    HandshakeResponse();

    HandshakeResponse(
        UInt32 capability_flags_, UInt32 max_packet_size_, UInt8 character_set_,
        const String & username_, const String & database_, const String & auth_response_, const String & auth_plugin_name_);
};

class AuthSwitchRequest : public IMySQLWritePacket
{
public:
    String plugin_name;
    String auth_plugin_data;

protected:
    size_t getPayloadSize() const override;

    void writePayloadImpl(WriteBuffer & buffer) const override;

public:
    AuthSwitchRequest(String plugin_name_, String auth_plugin_data_);
};

class AuthSwitchResponse : public LimitedReadPacket
{
public:
    String value;

protected:
    void readPayloadImpl(ReadBuffer & payload) override;
};

class AuthMoreData : public IMySQLWritePacket
{
public:
    String data;

protected:
    size_t getPayloadSize() const override;

    void writePayloadImpl(WriteBuffer & buffer) const override;

public:
    explicit AuthMoreData(String data_): data(std::move(data_)) {}
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

class AuthSwitchPacket : public IMySQLReadPacket
{
public:
    String plugin_name;

    AuthSwitchPacket() = default;

protected:
    UInt8 header = 0x00;

    void readPayloadImpl(ReadBuffer & payload) override;
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

enum ColumnType
{
    MYSQL_TYPE_DECIMAL = 0x00,
    MYSQL_TYPE_TINY = 0x01,
    MYSQL_TYPE_SHORT = 0x02,
    MYSQL_TYPE_LONG = 0x03,
    MYSQL_TYPE_FLOAT = 0x04,
    MYSQL_TYPE_DOUBLE = 0x05,
    MYSQL_TYPE_NULL = 0x06,
    MYSQL_TYPE_TIMESTAMP = 0x07,
    MYSQL_TYPE_LONGLONG = 0x08,
    MYSQL_TYPE_INT24 = 0x09,
    MYSQL_TYPE_DATE = 0x0a,
    MYSQL_TYPE_TIME = 0x0b,
    MYSQL_TYPE_DATETIME = 0x0c,
    MYSQL_TYPE_YEAR = 0x0d,
    MYSQL_TYPE_NEWDATE = 0x0e,
    MYSQL_TYPE_VARCHAR = 0x0f,
    MYSQL_TYPE_BIT = 0x10,
    MYSQL_TYPE_TIMESTAMP2 = 0x11,
    MYSQL_TYPE_DATETIME2 = 0x12,
    MYSQL_TYPE_TIME2 = 0x13,
    MYSQL_TYPE_JSON = 0xf5,
    MYSQL_TYPE_NEWDECIMAL = 0xf6,
    MYSQL_TYPE_ENUM = 0xf7,
    MYSQL_TYPE_SET = 0xf8,
    MYSQL_TYPE_TINY_BLOB = 0xf9,
    MYSQL_TYPE_MEDIUM_BLOB = 0xfa,
    MYSQL_TYPE_LONG_BLOB = 0xfb,
    MYSQL_TYPE_BLOB = 0xfc,
    MYSQL_TYPE_VAR_STRING = 0xfd,
    MYSQL_TYPE_STRING = 0xfe,
    MYSQL_TYPE_GEOMETRY = 0xff
};

class ColumnDefinitionPacket : public IMySQLWritePacket, public IMySQLReadPacket
{
public:
    String schema;
    String table;
    String org_table;
    String name;
    String org_name;
    size_t next_length = 0x0c;
    uint16_t character_set;
    uint32_t column_length;
    ColumnType column_type;
    uint16_t flags;
    uint8_t decimals = 0x00;

protected:
    size_t getPayloadSize() const override;

    void readPayloadImpl(ReadBuffer & payload) override;

    void writePayloadImpl(WriteBuffer & buffer) const override;

public:
    ColumnDefinitionPacket();

    ColumnDefinitionPacket(
        String schema_, String table_, String org_table_, String name_, String org_name_, uint16_t character_set_, uint32_t column_length_,
        ColumnType column_type_, uint16_t flags_, uint8_t decimals_);

    /// Should be used when column metadata (original name, table, original table, database) is unknown.
    ColumnDefinitionPacket(
        String name_, uint16_t character_set_, uint32_t column_length_, ColumnType column_type_, uint16_t flags_, uint8_t decimals_);

};

class ComFieldList : public LimitedReadPacket
{
public:
    String table, field_wildcard;

    void readPayloadImpl(ReadBuffer & payload) override;
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
