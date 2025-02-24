#pragma once

#include "HTTP2RequestHandlerFactory.h"

#include <Server/TCPServerConnectionFactory.h>
#include <Server/HTTP/HTTPContext.h>
#include <Common/ProfileEvents.h>

#include <Poco/Net/HTTPServerParams.h>

namespace DB
{

class HTTP2ServerConnectionFactory : public TCPServerConnectionFactory {
    public:
        explicit HTTP2ServerConnectionFactory(HTTP2RequestHandlerFactoryPtr factory);

        Poco::Net::TCPServerConnection * createConnection(const Poco::Net::StreamSocket & socket, TCPServer & tcp_server) override;

    private:
        // HTTPContextPtr context;
        // Poco::Net::HTTPServerParams::Ptr params;
        HTTP2RequestHandlerFactoryPtr factory;
        // ProfileEvents::Event read_event;
        // ProfileEvents::Event write_event;
    };

}
