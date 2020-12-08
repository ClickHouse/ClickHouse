#pragma once

#include <Poco/Net/TCPServerConnectionFactory.h>
#include <Poco/Net/NetException.h>
#include <common/logger_useful.h>
#include <Server/IServer.h>
#include <Server/TCPHandler.h>
#include <Server/TestKeeperTCPHandler.h>

namespace Poco { class Logger; }

namespace DB
{

class TCPHandlerFactory : public Poco::Net::TCPServerConnectionFactory
{
private:
    IServer & server;
    bool parse_proxy_protocol = false;
    Poco::Logger * log;
    bool test_keeper;

    class DummyTCPHandler : public Poco::Net::TCPServerConnection
    {
    public:
        using Poco::Net::TCPServerConnection::TCPServerConnection;
        void run() override {}
    };

public:
    /** parse_proxy_protocol_ - if true, expect and parse the header of PROXY protocol in every connection
      * and set the information about forwarded address accordingly.
      * See https://github.com/wolfeidau/proxyv2/blob/master/docs/proxy-protocol.txt
      */
    TCPHandlerFactory(IServer & server_, bool secure_, bool parse_proxy_protocol_, bool test_keeper_ = false)
        : server(server_), parse_proxy_protocol(parse_proxy_protocol_)
        , log(&Poco::Logger::get(std::string("TCP") + (secure_ ? "S" : "") + "HandlerFactory"))
        , test_keeper(test_keeper_)
    {
    }

    Poco::Net::TCPServerConnection * createConnection(const Poco::Net::StreamSocket & socket) override
    {
        try
        {
            LOG_TRACE(log, "TCP Request. Address: {}", socket.peerAddress().toString());

            if (test_keeper)
                return new TestKeeperTCPHandler(server, socket);
            else
                return new TCPHandler(server, socket, parse_proxy_protocol);
        }
        catch (const Poco::Net::NetException &)
        {
            LOG_TRACE(log, "TCP Request. Client is not connected (most likely RST packet was sent).");
            return new DummyTCPHandler(socket);
        }
    }
};

}
