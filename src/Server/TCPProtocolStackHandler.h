#pragma once

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
    bool access_denied;
    LoggerPtr log;

public:
    TCPProtocolStackHandler(IServer & server_, TCPServer & tcp_server_, const StreamSocket & socket, const std::vector<TCPServerConnectionFactory::Ptr> & stack_, const std::string & conf_name_, bool access_denied_ = false)
        : TCPServerConnection(socket), server(server_), tcp_server(tcp_server_), stack(stack_), conf_name(conf_name_), access_denied(access_denied_), log(getLogger("TCPProtocolStackHandler"))
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
                    std::string message = Exception::getMessageForErrorLog(ErrorCodes::IP_ADDRESS_NOT_ALLOWED, "IP address not allowed") + "\n";
                    
                    size_t total_sent = 0;
                    const size_t message_size = message.size();
                    while (total_sent < message_size)
                    {
                        int sent = socket().sendBytes(message.data() + total_sent, static_cast<int>(message_size - total_sent));
                        if (sent < 0)
                        {
                            LOG_ERROR(log, "Error while sending error message to blocked client (sendBytes returned {}, errno: {}).", sent, errno);
                            break;
                        }
                        if (sent == 0)
                        {
                            LOG_ERROR(log, "Connection closed by client while sending error message to blocked client (sent {} of {} bytes so far).", total_sent, message_size);
                            break;
                        }
                        total_sent += sent;
                    }
                    
                    if (total_sent != message_size)
                    {
                        LOG_ERROR(log, "Failed to send full error message to blocked client (sent {} of {} bytes).", total_sent, message_size);
                    }
                }
                catch (...)
                {
                    LOG_ERROR(log, "Failed to send error message to blocked client: {}", getCurrentExceptionMessage(false));
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
