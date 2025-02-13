#pragma once

#include <atomic>
#include <memory>
#include <Server/IServer.h>
#include <Server/TCPServerConnectionFactory.h>
#include <Core/PostgreSQLProtocol.h>
#include "config.h"

namespace DB
{

class MongoHandlerFactory : public TCPServerConnectionFactory
{
private:
    IServer & server;
    LoggerPtr log;
    ProfileEvents::Event read_event;
    ProfileEvents::Event write_event;

    std::atomic<Int32> last_connection_id = 0;
    std::vector<std::shared_ptr<PostgreSQLProtocol::PGAuthentication::AuthenticationMethod>> auth_methods;

public:
    explicit MongoHandlerFactory(
        IServer & server_,
        const ProfileEvents::Event & read_event_ = ProfileEvents::end(),
        const ProfileEvents::Event & write_event_ = ProfileEvents::end());

    Poco::Net::TCPServerConnection * createConnection(const Poco::Net::StreamSocket & socket, TCPServer & server) override;
};
}
