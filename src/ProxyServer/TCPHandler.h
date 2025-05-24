#pragma once

#include <Poco/Net/TCPServerConnection.h>

#include <IO/ReadBufferFromPocoSocketChunked.h>
#include <IO/WriteBufferFromPocoSocketChunked.h>
#include <Interpreters/ClientInfo.h>
#include <Server/TCPServer.h>

#include <ProxyServer/IProxyServer.h>
#include <ProxyServer/Router.h>

namespace Poco
{
class Logger;
}

namespace Proxy
{

using DB::TCPServer;

class TCPHandler : public Poco::Net::TCPServerConnection
{
private:
    struct ClientConnectionData
    {
        String user;
        String password;
        String default_database;
        String hostname;
        String client_name;
        UInt64 client_version_major = 0;
        UInt64 client_version_minor = 0;
        UInt64 client_version_patch = 0;
        UInt32 client_tcp_protocol_version = 0;
    };

public:
    TCPHandler(
        IProxyServer & server_,
        TCPServer & tcp_server_,
        const Poco::Net::StreamSocket & socket_,
        bool parse_proxy_protocol_,
        RouterPtr router_);
    ~TCPHandler() override;

    void run() override;

private:
    IProxyServer & server;
    TCPServer & tcp_server;
    bool parse_proxy_protocol = false;
    LoggerPtr log;

    [[maybe_unused]] String forwarded_for;

    Poco::Timespan send_timeout = Poco::Timespan(DB::DBMS_DEFAULT_SEND_TIMEOUT_SEC, 0);
    Poco::Timespan receive_timeout = Poco::Timespan(DB::DBMS_DEFAULT_RECEIVE_TIMEOUT_SEC, 0);

    /// Streams for reading/writing from/to client connection socket.
    std::shared_ptr<DB::ReadBufferFromPocoSocketChunked> in;
    std::shared_ptr<DB::WriteBufferFromPocoSocketChunked> out;

    RouterPtr router;

    std::optional<Action> action;

    std::string proxy_header;
    ClientConnectionData client_connection_data;
    std::unique_ptr<Poco::Net::StreamSocket> target_socket;
    /// Stream for writing to server connection socket.
    std::shared_ptr<DB::WriteBufferFromPocoSocketChunked> target_out;

    void runImpl();

    void generateProxyHeader();

    bool receiveProxyHeader();
    void sendProxyHeader();
    void receiveHello();
    void redirectHello();
    void connect();

    void doRedirection();
};

}
