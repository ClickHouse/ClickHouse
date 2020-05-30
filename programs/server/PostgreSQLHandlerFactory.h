#pragma once

#include "IServer.h"
#include <Poco/Net/TCPServerConnectionFactory.h>
#include <Core/PostgreSQLProtocol.h>

namespace DB
{

class PostgreSQLHandlerFactory : public Poco::Net::TCPServerConnectionFactory
{
private:
#if USE_SSL
    IServer & server;
    Poco::Logger * log;
    bool ssl_enabled = true;
#else
    bool ssl_enabled = false;
#endif

    std::atomic<Int32> last_connection_id = 0;
    std::vector<std::shared_ptr<PostgreSQLProtocol::PGAuthentication::AuthenticationMethod>> auth_methods;

public:
    explicit PostgreSQLHandlerFactory(IServer & server_);

    Poco::Net::TCPServerConnection * createConnection(const Poco::Net::StreamSocket & socket) override;
};
}
