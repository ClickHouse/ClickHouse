#include "HTTP2ServerConnectionFactory.h"

#include "HTTP2ServerConnection.h"

namespace DB
{
    HTTP2ServerConnectionFactory::HTTP2ServerConnectionFactory(
        HTTP2RequestHandlerFactoryPtr factory_
        )
    : factory(factory_)
    {
        poco_check_ptr(factory);
    }

    Poco::Net::TCPServerConnection * HTTP2ServerConnectionFactory::createConnection(const Poco::Net::StreamSocket & socket, TCPServer & tcp_server)
    {
        UNUSED(tcp_server);
        return new HTTP2ServerConnection(socket, factory);
    }

}
