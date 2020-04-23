#pragma once
#include <Core/MySQLProtocol.h>
#include <Core/Types.h>
#include <IO/ReadBufferFromPocoSocket.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromPocoSocket.h>
#include <IO/WriteHelpers.h>
#include <Poco/Net/NetException.h>
#include <Poco/Net/StreamSocket.h>
#include <Common/DNSResolver.h>
#include <Common/Exception.h>
#include <Common/NetException.h>


namespace DB
{
using namespace MySQLProtocol;

class MySQLClient
{
public:
    MySQLClient(const String & host_, UInt16 port_, const String & user_, const String & password_, const String & database_);
    bool connect();
    void disconnect();
    bool ping();
    bool initdb(String db);
    bool register_slave(UInt32 server_id);
    String error();

private:
    String host;
    UInt16 port;
    String user;
    String password;
    String database;

    bool connected = false;
    String last_error;
    UInt32 client_capability_flags = 0;

    uint8_t seq = 0;
    UInt8 charset_utf8 = 33;
    UInt32 max_packet_size = MySQLProtocol::MAX_PACKET_LENGTH;
    String mysql_native_password = "mysql_native_password";

    std::shared_ptr<ReadBuffer> in;
    std::shared_ptr<WriteBuffer> out;
    std::unique_ptr<Poco::Net::StreamSocket> socket;
    std::optional<Poco::Net::SocketAddress> address;
    std::shared_ptr<PacketSender> packet_sender;

    bool handshake();
    bool writeCommand(char command, String query);
};

class WriteCommand : public WritePacket
{
public:
    char command;
    String query;

    WriteCommand(char command_, String query_) : command(command_), query(query_) { }

    size_t getPayloadSize() const override { return 1 + query.size(); }

    void writePayloadImpl(WriteBuffer & buffer) const override
    {
        buffer.write(static_cast<char>(command));
        if (!query.empty())
        {
            buffer.write(query.data(), query.size());
        }
    }
};
}
