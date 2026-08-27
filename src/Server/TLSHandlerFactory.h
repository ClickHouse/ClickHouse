#pragma once

#include <Poco/Logger.h>
#include <Poco/Net/TCPServerConnection.h>
#include <Poco/Net/NetException.h>
#include <Poco/Util/LayeredConfiguration.h>
#include <Server/TLSHandler.h>
#include <Server/IServer.h>
#include <Server/TCPServer.h>
#include <Server/TCPProtocolStackData.h>
#include <Common/logger_useful.h>


namespace DB
{


class TLSHandlerFactory : public TCPServerConnectionFactory
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
    explicit TLSHandlerFactory(IServer & server_, const std::string & conf_name_)
        : server(server_), log(getLogger("TLSHandlerFactory")), conf_name(conf_name_)
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
            return new TLSHandler(
                socket,
                server.config(),
                conf_name + ".",
                stack_data);
        }
        catch (const Poco::Net::NetException &)
        {
            LOG_TRACE(log, "TCP Request. Client is not connected (most likely RST packet was sent).");
            return new DummyTCPHandler(socket);
        }
    }
};


}
