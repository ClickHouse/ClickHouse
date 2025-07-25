#pragma once

#include <atomic>
#include <memory>
#include <Server/IServer.h>
#include <Server/TCPServerConnectionFactory.h>
#include <Core/PostgreSQLProtocol.h>
#include "config.h"

namespace DB
{

class PostgreSQLHandlerFactory : public TCPServerConnectionFactory
{
private:
    IServer & server;
    LoggerPtr log;
    ProfileEvents::Event read_event;
    ProfileEvents::Event write_event;

#if USE_SSL
    std::string conf_name;

    bool ssl_enabled = true;
#else
    bool ssl_enabled = false;
#endif

    bool secure_required = false;

    std::atomic<Int32> last_connection_id = 0;
    std::vector<std::shared_ptr<PostgreSQLProtocol::PGAuthentication::AuthenticationMethod>> auth_methods;

public:
    explicit PostgreSQLHandlerFactory(
        IServer & server_,
        bool secure_required_,
#if USE_SSL
        const std::string & conf_name_,
#endif
        const ProfileEvents::Event & read_event_ = ProfileEvents::end(),
        const ProfileEvents::Event & write_event_ = ProfileEvents::end());

    Poco::Net::TCPServerConnection * createConnection(const Poco::Net::StreamSocket & socket, TCPServer & server) override;
};
}
