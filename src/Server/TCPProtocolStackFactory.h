#pragma once

#include <Server/TCPServerConnectionFactory.h>
#include <Server/IServer.h>
#include <Server/TCPProtocolStackHandler.h>


namespace DB
{


class TCPProtocolStackFactory : public TCPServerConnectionFactory
{
private:
    IServer & server [[maybe_unused]];
    Poco::Logger * log;
    std::string conf_name;
    std::list<TCPServerConnectionFactory::Ptr> stack;

    class DummyTCPHandler : public Poco::Net::TCPServerConnection
    {
    public:
        using Poco::Net::TCPServerConnection::TCPServerConnection;
        void run() override {}
    };

public:
    template <typename... T>
    explicit TCPProtocolStackFactory(IServer & server_, const std::string & conf_name_, T... factory)
        : server(server_), log(&Poco::Logger::get("TCPProtocolStackFactory")), conf_name(conf_name_), stack({factory...})
    {
    }

    Poco::Net::TCPServerConnection * createConnection(const Poco::Net::StreamSocket & socket, TCPServer & tcp_server) override
    {
        try
        {
            LOG_TRACE(log, "TCP Request. Address: {}", socket.peerAddress().toString());
            return new TCPProtocolStackHandler(tcp_server, socket, stack, conf_name);
        }
        catch (const Poco::Net::NetException &)
        {
            LOG_TRACE(log, "TCP Request. Client is not connected (most likely RST packet was sent).");
            return new DummyTCPHandler(socket);
        }
    }

    void append(TCPServerConnectionFactory::Ptr factory)
    {
        stack.push_back(factory);
    }

    size_t size() { return stack.size(); }
};


}
