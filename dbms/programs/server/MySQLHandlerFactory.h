#pragma once

#include <Poco/Net/TCPServerConnectionFactory.h>
#include <common/logger_useful.h>
#include "IServer.h"
#include "MySQLHandler.h"

namespace Poco { class Logger; }

namespace DB
{

    class MySQLHandlerFactory : public Poco::Net::TCPServerConnectionFactory
    {
    private:
        IServer & server;
        Poco::Logger * log;

    public:
        explicit MySQLHandlerFactory(IServer & server_)
            : server(server_)
            , log(&Logger::get("MySQLHandlerFactory"))
        {
        }

        Poco::Net::TCPServerConnection * createConnection(const Poco::Net::StreamSocket & socket) override
        {
            LOG_TRACE(log, "MySQL connection. Address: " << socket.peerAddress().toString());
            return new MySQLHandler(server, socket);
        }
    };

}
