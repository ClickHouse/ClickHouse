#pragma once

#include <atomic>
#include <memory>
#include <Server/IServer.h>
#include <Server/TCPServerConnectionFactory.h>
#include <Common/ProfileEvents.h>
// #include <Core/PostgreSQLProtocol.h>
#include "config.h"

namespace DB
{

class MongoDBHandlerFactory : public TCPServerConnectionFactory
{
private:
    IServer & server;
    LoggerPtr log;
    ProfileEvents::Event read_event;
    ProfileEvents::Event write_event;
    constexpr static const auto factory_name = "MongoDBHandlerFactory";

#if USE_SSL
    bool ssl_enabled = true;
#else
    bool ssl_enabled = false;
#endif

    std::atomic<Int32> last_connection_id = 0;

public:
    explicit MongoDBHandlerFactory(IServer & server_);

    Poco::Net::TCPServerConnection * createConnection(const Poco::Net::StreamSocket & socket, TCPServer & server) override;
};
}
