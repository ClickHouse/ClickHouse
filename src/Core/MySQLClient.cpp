#include <Core/MySQLClient.h>

namespace DB
{
using namespace MySQLProtocol;
using namespace MySQLProtocol::Authentication;

namespace ErrorCodes
{
    extern const int NETWORK_ERROR;
    extern const int SOCKET_TIMEOUT;
    extern const int UNKNOWN_PACKET_FROM_SERVER;
}

MySQLClient::MySQLClient(const String & _host, UInt16 _port, const String & _user, const String & _password, const String & _database)
    : host(_host), port(_port), user(_user), password(_password), database(_database)
{
    client_capability_flags = CLIENT_PROTOCOL_41 | CLIENT_PLUGIN_AUTH | CLIENT_SECURE_CONNECTION;
}

void MySQLClient::connect()
{
    try
    {
        if (connected)
        {
            close();
        }

        socket = std::make_unique<Poco::Net::StreamSocket>();
        address = DNSResolver::instance().resolveAddress(host, port);
        socket->connect(*address);

        in = std::make_shared<ReadBufferFromPocoSocket>(*socket);
        out = std::make_shared<WriteBufferFromPocoSocket>(*socket);
        packet_sender = std::make_shared<PacketSender>(*in, *out, seq);
        connected = true;

        handshake(*in);
    }
    catch (Poco::Net::NetException & e)
    {
        close();
        throw NetException(e.displayText(), ErrorCodes::NETWORK_ERROR);
    }
    catch (Poco::TimeoutException & e)
    {
        close();
        throw NetException(e.displayText(), ErrorCodes::SOCKET_TIMEOUT);
    }
}

void MySQLClient::close()
{
    in = nullptr;
    out = nullptr;
    if (socket)
        socket->close();
    socket = nullptr;
    connected = false;
}

/// https://dev.mysql.com/doc/internals/en/connection-phase-packets.html
void MySQLClient::handshake(ReadBuffer & payload)
{
    Handshake handshake;
    handshake.readPayloadImpl(payload);
    if (handshake.auth_plugin_name != mysql_native_password)
    {
        throw Exception(
            "Only support " + mysql_native_password + " auth plugin name, but got " + handshake.auth_plugin_name,
            ErrorCodes::UNKNOWN_PACKET_FROM_SERVER);
    }

    Native41 native41(password, handshake.auth_plugin_data);
    String response = native41.getAuthPluginData();

    HandshakeResponse handshakeResponse(client_capability_flags, 0, charset_utf8, user, database, handshake.auth_plugin_data, mysql_native_password);
    packet_sender->sendPacket<HandshakeResponse>(handshakeResponse, true);
}
}
