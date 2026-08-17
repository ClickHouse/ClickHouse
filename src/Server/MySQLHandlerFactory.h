#pragma once

#include <Server/IServer.h>
#include <Server/TCPServerConnectionFactory.h>
#include <Common/ProfileEvents.h>

#include "config.h"

#include <atomic>

#if USE_SSL
#    include <Common/Crypto/KeyPair.h>
#endif

namespace DB
{
class TCPServer;

class MySQLHandlerFactory : public TCPServerConnectionFactory
{
private:
    IServer & server;
    LoggerPtr log;

#if USE_SSL
    KeyPair keypair;

    bool ssl_enabled = true;
#else
    bool ssl_enabled = false;
#endif

    bool secure_required = false;

    std::atomic<unsigned> last_connection_id = 0;

    ProfileEvents::Event read_event;
    ProfileEvents::Event write_event;
public:
    explicit MySQLHandlerFactory(IServer & server_, bool secure_required_, const ProfileEvents::Event & read_event_ = ProfileEvents::end(), const ProfileEvents::Event & write_event_ = ProfileEvents::end());

    void readRSAKeys();

    void generateRSAKeys();

    Poco::Net::TCPServerConnection * createConnection(const Poco::Net::StreamSocket & socket, TCPServer & tcp_server) override;
};

}
