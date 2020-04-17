#pragma once
#include <Core/Types.h>
#include <Core/MySQLProtocol.h>
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
class MySQLClient
{
public:
    MySQLClient(const String & host_, UInt16 port_, const String & user_, const String & password_, const String & database_);

    void connect();

    void close();

private:
    String host;
    UInt16 port;
    String user;
    String password;
    String database;

    bool connected = false;
    UInt32 client_capability_flags = 0;

    uint8_t  seq = 0;
    UInt8 charset_utf8 = 33;
    String mysql_native_password = "mysql_native_password";

    std::shared_ptr<ReadBuffer> in;
    std::shared_ptr<WriteBuffer> out;
    std::unique_ptr<Poco::Net::StreamSocket> socket;
    std::optional<Poco::Net::SocketAddress> address;

    void handshake(ReadBuffer & payload);

protected:
    std::shared_ptr<MySQLProtocol::PacketSender> packet_sender;
};
}
