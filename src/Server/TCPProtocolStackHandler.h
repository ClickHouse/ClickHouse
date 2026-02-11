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
        catch (...) // NOLINT(bugprone-empty-catch)
        {
            peer_address_str = "unknown";
        }

        /// Keep the secure connection alive if we need to send an error message afterwards.
        std::unique_ptr<TCPServerConnection> secure_connection_holder;

        for (auto & factory : stack)
        {
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

                        /// SecureStreamSocket does not perform handshake until first I/O.
                        /// We MUST force it here to catch handshake failures and ensure we can send the error message.
                        socket().receiveBytes(nullptr, 0); // Peek/poke to trigger handshake
                        secure_connection_holder = std::move(connection);
                        handshake_ok = true;
                    }
                    catch (...) // NOLINT(bugprone-empty-catch)
                    {
                        LOG_WARNING(log, "TLS handshake failed for blocked client {}: {}", peer_address_str, getCurrentExceptionMessage(false));
                    }

                    if (!handshake_ok)
                        return;

                    continue;
                }

                /// If we are here, we are at the first non-secure layer (protocol layer) and IP is blocked.
                try
                {
                    std::string message = "Code: " + std::to_string(ErrorCodes::IP_ADDRESS_NOT_ALLOWED) + ". DB::Exception: IP address not allowed.\n";
                    LOG_DEBUG(log, "IP address {} is blocked. Sending error message.", peer_address_str);

                    int sent = 0;
                    const int total_size = static_cast<int>(message.size());
                    while (sent < total_size)
                    {
                        int n = socket().sendBytes(message.data() + sent, total_size - sent);
                        if (n <= 0)
                            break;
                        sent += n;
                    }

                    if (!secure_connection_holder)
                    {
                        try { socket().shutdownSend(); }
                        catch (...) { /* ignore */ } // NOLINT(bugprone-empty-catch)
                    }

                    LOG_DEBUG(log, "Sent {} bytes of error message to {}.", sent, peer_address_str);

                    /// Drain for a bit to ensure the client gets the message and doesn't get a RST.
                    auto end_time = std::chrono::steady_clock::now() + std::chrono::seconds(2);
                    while (!server.isCancelled() && std::chrono::steady_clock::now() < end_time)
                    {
                        if (socket().poll(Poco::Timespan(100000), Poco::Net::Socket::SELECT_READ))
                        {
                            char buffer[1024];
                            try { if (socket().receiveBytes(buffer, sizeof(buffer)) <= 0) break; }
                            catch (...) { break; } // NOLINT(bugprone-empty-catch)
                        }
                    }

                    socket().close();
                }
                catch (...) // NOLINT(bugprone-empty-catch)
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
