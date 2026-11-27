#pragma once

#include <Poco/SharedPtr.h>
#include <Server/TCPProtocolStackData.h>
#include <Common/SignalHandlers.h>


namespace Poco
{
namespace Net
{
    class StreamSocket;
    class TCPServerConnection;
}
}

namespace DB
{

class TCPServer;

class TCPServerConnectionFactory
{
public:
    using Ptr = Poco::SharedPtr<TCPServerConnectionFactory>;

    virtual ~TCPServerConnectionFactory() = default;

    Poco::Net::TCPServerConnection * createConnection(const Poco::Net::StreamSocket & socket, TCPServer & tcp_server)
    {
        /// Refuse connections as soon as server is crashed.
        if (isCrashed())
            return nullptr;
        return createConnectionImpl(socket, tcp_server);
    }

    Poco::Net::TCPServerConnection * createConnection(const Poco::Net::StreamSocket & socket, TCPServer & tcp_server, TCPProtocolStackData & stack_data)
    {
        if (isCrashed())
            return nullptr;
        return createConnectionImpl(socket, tcp_server, stack_data);
    }

    /// Same as Poco::Net::TCPServerConnectionFactory except we can pass the TCPServer
    virtual Poco::Net::TCPServerConnection * createConnectionImpl(const Poco::Net::StreamSocket & socket, TCPServer & tcp_server) = 0;
    virtual Poco::Net::TCPServerConnection * createConnectionImpl(const Poco::Net::StreamSocket & socket, TCPServer & tcp_server, TCPProtocolStackData &/* stack_data */)
    {
        return createConnectionImpl(socket, tcp_server);
    }
};

}
