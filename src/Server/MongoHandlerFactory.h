#pragma once

#include <atomic>
#include <optional>
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
    /// If set, overrides the `default_session_user` server setting for this listener.
    std::optional<String> default_session_user;

    std::atomic<Int32> last_connection_id = 0;

public:
    explicit MongoHandlerFactory(
        IServer & server_,
        const ProfileEvents::Event & read_event_ = ProfileEvents::end(),
        const ProfileEvents::Event & write_event_ = ProfileEvents::end(),
        std::optional<String> default_session_user_ = {});

    Poco::Net::TCPServerConnection * createConnectionImpl(const Poco::Net::StreamSocket & socket, TCPServer & server) override;
};
}

#endif
