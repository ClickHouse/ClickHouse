#pragma once

#include <Poco/SharedPtr.h>
#include <Server/TCPProtocolStackData.h>

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

    /// Same as Poco::Net::TCPServerConnectionFactory except we can pass the TCPServer
    virtual Poco::Net::TCPServerConnection * createConnection(const Poco::Net::StreamSocket & socket, TCPServer & tcp_server) = 0;
    virtual Poco::Net::TCPServerConnection * createConnection(const Poco::Net::StreamSocket & socket, TCPServer & tcp_server, TCPProtocolStackData &/* stack_data */)
    {
        return createConnection(socket, tcp_server);
    }
};
}
