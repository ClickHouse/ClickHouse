#pragma once

#include <Poco/Net/NetException.h>
#include <Poco/Net/TCPServerConnection.h>
#include <Server/ProxyV1Handler.h>
#include <Common/logger_useful.h>
#include <Server/IServer.h>
#include <Server/TCPServer.h>
#include <Server/TCPProtocolStackData.h>


namespace DB
{

class ProxyV1HandlerFactory : public TCPServerConnectionFactory
{
private:
    IServer & server;
    LoggerPtr log;
    std::string conf_name;

    class DummyTCPHandler : public Poco::Net::TCPServerConnection
    {
    public:
        using Poco::Net::TCPServerConnection::TCPServerConnection;
        void run() override {}
    };

public:
    explicit ProxyV1HandlerFactory(IServer & server_, const std::string & conf_name_)
        : server(server_), log(getLogger("ProxyV1HandlerFactory")), conf_name(conf_name_)
    {
    }

    Poco::Net::TCPServerConnection * createConnection(const Poco::Net::StreamSocket & socket, TCPServer & tcp_server) override
    {
        TCPProtocolStackData stack_data;
        return createConnection(socket, tcp_server, stack_data);
    }

    Poco::Net::TCPServerConnection * createConnection(const Poco::Net::StreamSocket & socket, TCPServer &/* tcp_server*/, TCPProtocolStackData & stack_data) override
    {
        try
        {
            LOG_TRACE(log, "TCP Request. Address: {}", socket.peerAddress().toString());
            return new ProxyV1Handler(socket, server, conf_name, stack_data);
        }
        catch (const Poco::Net::NetException &)
        {
            LOG_TRACE(log, "TCP Request. Client is not connected (most likely RST packet was sent).");
            return new DummyTCPHandler(socket);
        }
    }
};

}
