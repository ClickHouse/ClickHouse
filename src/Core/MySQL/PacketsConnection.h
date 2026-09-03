#pragma once

#include <Core/MySQL/IMySQLReadPacket.h>
#include <Core/MySQL/IMySQLWritePacket.h>

namespace DB
{

namespace MySQLProtocol
{

namespace ConnectionPhase
{

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

    Handshake(
        uint32_t capability_flags_, uint32_t connection_id_,
        String server_version_, String auth_plugin_name_, String auth_plugin_data_, uint8_t charset_);
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

}

}

}
