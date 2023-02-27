#pragma once

#include <Server/TCPServerConnectionFactory.h>
#include <Server/TCPServer.h>
#include <Poco/Util/LayeredConfiguration.h>
#include <Server/IServer.h>
#include <Server/TCPProtocolStackData.h>


namespace DB
{


class TCPProtocolStackHandler : public Poco::Net::TCPServerConnection
{
    using StreamSocket = Poco::Net::StreamSocket;
    using TCPServerConnection = Poco::Net::TCPServerConnection;
private:
    IServer & server;
    TCPServer & tcp_server;
    std::vector<TCPServerConnectionFactory::Ptr> stack;
    std::string conf_name;

public:
    TCPProtocolStackHandler(IServer & server_, TCPServer & tcp_server_, const StreamSocket & socket, const std::vector<TCPServerConnectionFactory::Ptr> & stack_, const std::string & conf_name_)
        : TCPServerConnection(socket), server(server_), tcp_server(tcp_server_), stack(stack_), conf_name(conf_name_)
    {}

    void run() override
    {
        const auto & conf = server.config();
        TCPProtocolStackData stack_data;
        stack_data.socket = socket();
        stack_data.default_database = conf.getString(conf_name + ".default_database", "");
        for (auto & factory : stack)
        {
            std::unique_ptr<TCPServerConnection> connection(factory->createConnection(socket(), tcp_server, stack_data));
            connection->run();
            if (stack_data.socket != socket())
                socket() = stack_data.socket;
        }
    }
};


}
