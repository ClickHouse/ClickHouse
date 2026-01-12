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
    bool access_denied;

public:
    TCPProtocolStackHandler(IServer & server_, TCPServer & tcp_server_, const StreamSocket & socket, const std::vector<TCPServerConnectionFactory::Ptr> & stack_, const std::string & conf_name_, bool access_denied_ = false)
        : TCPServerConnection(socket), server(server_), tcp_server(tcp_server_), stack(stack_), conf_name(conf_name_), access_denied(access_denied_)
    {}

    void run() override
    {
        const auto & conf = server.config();
        TCPProtocolStackData stack_data;
        stack_data.socket = socket();
        stack_data.default_database = conf.getString(conf_name + ".default_database", "");
        for (auto & factory : stack)
        {
            /// If the IP is denied, we only allow TLS handshake to pass through to send a proper error message.
            if (access_denied)
            {
                if (factory->isSecure())
                {
                    std::unique_ptr<TCPServerConnection> connection(factory->createConnection(socket(), tcp_server, stack_data));
                    connection->run();
                    if (stack_data.socket != socket())
                        socket() = stack_data.socket;
                    continue;
                }

                /// If we are here, it means we have passed the TLS layer (if any) and now we are at the protocol layer.
                /// We should send an error message and close the connection.
                try
                {
                    /// Mimic standard ClickHouse exception message for ErrorCodes::IP_ADDRESS_NOT_ALLOWED (195)
                    std::string message = "Code: 195. DB::Exception: IP address not allowed.\n";
                    socket().sendBytes(message.data(), static_cast<int>(message.size()));
                }
                catch (...)
                {
                }
                return;
            }

            std::unique_ptr<TCPServerConnection> connection(factory->createConnection(socket(), tcp_server, stack_data));
            connection->run();
            if (stack_data.socket != socket())
                socket() = stack_data.socket;
        }
    }
};


}
