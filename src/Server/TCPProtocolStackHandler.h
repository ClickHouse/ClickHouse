#pragma once

#include <thread>
#include <chrono>
#include <memory>
#include <vector>
#include <string>
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

        std::string peer_address_str;
        try
        {
            peer_address_str = stack_data.socket.peerAddress().toString();
        }
        catch (...)
        {
            peer_address_str = "unknown";
        }

        /// Keep the secure connection alive if we need to send an error message afterwards.
        std::unique_ptr<TCPServerConnection> secure_connection_holder;

        for (auto & factory : stack)
        {
            /// If the IP is denied, we only allow TLS handshake to pass through to send a proper error message.
            if (ip_blocked)
            {
                if (factory->isSecure())
                {
                    bool handshake_ok = false;
                    try
                    {
                        std::unique_ptr<TCPServerConnection> connection(factory->createConnection(socket(), tcp_server, stack_data));
                        connection->run();
                        if (stack_data.socket != socket())
                            socket() = stack_data.socket;
                        // Move ownership to keep it alive
                        secure_connection_holder = std::move(connection);
                        handshake_ok = true;
                    }
                    catch (...)
                    {
                        LOG_WARNING(log, "TLS handshake failed for blocked client {}: {}", peer_address_str, getCurrentExceptionMessage(false));
                    }

                    if (!handshake_ok)
                        return;

                    continue;
                }

                /// If we are here, it means we have passed the TLS layer (if any) and now we are at the protocol layer.
                /// We should send an error message and close the connection.
                try
                {
                    std::string message = "Code: " + std::to_string(ErrorCodes::IP_ADDRESS_NOT_ALLOWED) + ". DB::Exception: IP address not allowed.\n";
                    LOG_DEBUG(log, "IP address {} is blocked. Sending error message.", peer_address_str);

                    int sent = stack_data.socket.sendBytes(message.data(), static_cast<int>(message.size()));
                    stack_data.socket.shutdownSend();
                    LOG_DEBUG(log, "Sent {} bytes of error message.", sent);

                    /// Wait for the client to close the connection or timeout, while draining the socket to prevent RST.
                    auto start = std::chrono::steady_clock::now();
                    while (!server.isCancelled() && std::chrono::steady_clock::now() - start < std::chrono::seconds(2))
                    {
                        if (stack_data.socket.poll(Poco::Timespan(100000), Poco::Net::Socket::SELECT_READ))
                        {
                            char buffer[4096];
                            int n = stack_data.socket.receiveBytes(buffer, sizeof(buffer));
                            if (n <= 0) break; /// Client closed or error.
                            LOG_DEBUG(log, "Drained {} bytes from blocked client.", n);
                        }
                    }
                    if (sent != static_cast<int>(message.size()))
                    {
                        LOG_ERROR(log, "Failed to send complete IP block error message to client {} (sent {} of {} bytes).", peer_address_str, sent, message.size());
                    }
                    else
                    {
                        LOG_INFO(log, "Sent IP access denied message to {}.", peer_address_str);
                    }

                    stack_data.socket.close();
                }
                catch (...)
                {
                    LOG_ERROR(log, "Failed to send error message to blocked client {}: {}", peer_address_str, getCurrentExceptionMessage(false));
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
