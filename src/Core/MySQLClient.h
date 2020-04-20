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
    void close();
    String error();

private:
    String host;
    UInt16 port;
    String user;
    String password;
    String database;

    bool connected = false;
    UInt32 client_capability_flags = 0;
    String last_error;

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
};
}
