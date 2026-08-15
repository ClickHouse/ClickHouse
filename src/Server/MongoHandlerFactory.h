#pragma once

#include <atomic>
#include <Server/IServer.h>
#include <Server/TCPServerConnectionFactory.h>
#include "config.h"

/// The Mongo wire protocol needs BSON (mongo-cxx-driver) and the Mongo dialect (rapidjson).
#if USE_MONGODB && USE_RAPIDJSON

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

public:
    explicit MongoHandlerFactory(
        IServer & server_,
        const ProfileEvents::Event & read_event_ = ProfileEvents::end(),
        const ProfileEvents::Event & write_event_ = ProfileEvents::end());

    Poco::Net::TCPServerConnection * createConnectionImpl(const Poco::Net::StreamSocket & socket, TCPServer & server) override;
};
}

#endif
