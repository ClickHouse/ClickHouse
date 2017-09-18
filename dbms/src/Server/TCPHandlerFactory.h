#pragma once

#include <Poco/Net/TCPServerConnectionFactory.h>
#include <common/logger_useful.h>
#include "IServer.h"
#include "TCPHandler.h"

namespace Poco { class Logger; }

namespace DB
{

class TCPHandlerFactory : public Poco::Net::TCPServerConnectionFactory
{
private:
    IServer & server;
    Poco::Logger * log;

public:
    explicit TCPHandlerFactory(IServer & server_)
        : server(server_)
        , log(&Logger::get("TCPHandlerFactory"))
    {
    }

    Poco::Net::TCPServerConnection * createConnection(const Poco::Net::StreamSocket & socket) override
    {
        LOG_TRACE(log,
            "TCP Request. "
                << "Address: "
                << socket.peerAddress().toString());

        return new TCPHandler(server, socket);
    }
};

}
