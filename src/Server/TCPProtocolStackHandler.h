#pragma once

#include <thread>
#include <chrono>

#include <Server/TCPServerConnectionFactory.h>
#include <Server/TCPServer.h>
#include <Poco/Util/LayeredConfiguration.h>
#include <Server/IServer.h>
#include <Server/TCPProtocolStackData.h>
#include <Common/logger_useful.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int IP_ADDRESS_NOT_ALLOWED;
}

class TCPProtocolStackHandler : public Poco::Net::TCPServerConnection
{
    using StreamSocket = Poco::Net::StreamSocket;
    using TCPServerConnection = Poco::Net::TCPServerConnection;
private:
    IServer & server;
    TCPServer & tcp_server;
    std::vector<TCPServerConnectionFactory::Ptr> stack;
    std::string conf_name;
    bool ip_blocked;
    LoggerPtr log;

public:
    TCPProtocolStackHandler(IServer & server_, TCPServer & tcp_server_, const StreamSocket & socket, const std::vector<TCPServerConnectionFactory::Ptr> & stack_, const std::string & conf_name_, bool ip_blocked_ = false)
        : TCPServerConnection(socket)
        , server(server_)
        , tcp_server(tcp_server_)
        , stack(stack_)
        , conf_name(conf_name_)
        , ip_blocked(ip_blocked_)
        , log(getLogger("TCPProtocolStackHandler"))
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
            if (ip_blocked)
            {
                if (factory->isSecure())
                {
                    try
                    {
                        std::unique_ptr<TCPServerConnection> connection(factory->createConnection(socket(), tcp_server, stack_data));
                        connection->run();
                        if (stack_data.socket != socket())
                            socket() = stack_data.socket;
                    }
                    catch (...)
                    {
                        LOG_WARNING(log, "TLS handshake failed for blocked client {}: {}", socket().peerAddress().toString(), getCurrentExceptionMessage(false));
                    }
                    continue;
                }

                /// If we are here, it means we have passed the TLS layer (if any) and now we are at the protocol layer.
                /// We should send an error message and close the connection.
                try
                {
                    std::string message = "Code: " + std::to_string(ErrorCodes::IP_ADDRESS_NOT_ALLOWED) + ". DB::Exception: IP address not allowed.\n";
                    
                    /// Drain the receive buffer to prevent RST when we close.
                    char buffer[1024];
                    while (socket().poll(Poco::Timespan(0, 1000), Poco::Net::Socket::SELECT_READ))
                    {
                         int n = socket().receiveBytes(buffer, sizeof(buffer));
                         if (n <= 0) break;
                    }

                    int sent = socket().sendBytes(message.data(), static_cast<int>(message.size()));
                    socket().shutdownSend();
                    std::this_thread::sleep_for(std::chrono::seconds(1));
                    if (sent != static_cast<int>(message.size()))
                    {
                        LOG_ERROR(log, "Failed to send complete IP block error message to client {} (sent {} of {} bytes).", socket().peerAddress().toString(), sent, message.size());
                    }
                }
                catch (...)
                {
                    LOG_ERROR(log, "Failed to send error message to blocked client {}: {}", socket().peerAddress().toString(), getCurrentExceptionMessage(false));
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
