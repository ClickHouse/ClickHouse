#pragma once

#include <atomic>
#include <memory>
#include <Server/IServer.h>
#include <Server/TCPServerConnectionFactory.h>
#include <Common/ProfileEvents.h>

#include "config.h"

#if USE_SSL
#    include <openssl/rsa.h>
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
    struct RSADeleter
    {
        void operator()(RSA * ptr) { RSA_free(ptr); }
    };
    using RSAPtr = std::unique_ptr<RSA, RSADeleter>;

    RSAPtr public_key;
    RSAPtr private_key;

    bool ssl_enabled = true;
#else
    bool ssl_enabled = false;
#endif

    std::atomic<unsigned> last_connection_id = 0;

    ProfileEvents::Event read_event;
    ProfileEvents::Event write_event;
public:
    explicit MySQLHandlerFactory(IServer & server_, const ProfileEvents::Event & read_event_ = ProfileEvents::end(), const ProfileEvents::Event & write_event_ = ProfileEvents::end());

    void readRSAKeys();

    void generateRSAKeys();

    Poco::Net::TCPServerConnection * createConnection(const Poco::Net::StreamSocket & socket, TCPServer & tcp_server) override;
};

}
