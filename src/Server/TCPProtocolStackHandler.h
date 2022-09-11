#pragma once

#include <list>
#include <Server/TCPServerConnectionFactory.h>
#include <Server/TCPServer.h>
#include "Server/TCPProtocolStackData.h"


namespace DB
{


class TCPProtocolStackHandler : public Poco::Net::TCPServerConnection
{
    using StreamSocket = Poco::Net::StreamSocket;
    using TCPServerConnection = Poco::Net::TCPServerConnection;
private:
    TCPServer & tcp_server;
    std::list<TCPServerConnectionFactory::Ptr> stack;
    std::string conf_name;

public:
    TCPProtocolStackHandler(TCPServer & tcp_server_, const StreamSocket & socket, const std::list<TCPServerConnectionFactory::Ptr> & stack_, const std::string & conf_name_)
        : TCPServerConnection(socket), tcp_server(tcp_server_), stack(stack_), conf_name(conf_name_)
    {}

    void run() override
    {
        TCPProtocolStackData stack_data;
        stack_data.socket = socket();
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
