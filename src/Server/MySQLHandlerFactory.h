#pragma once

#include <atomic>
#include <memory>
#include <Server/IServer.h>
#include <Server/TCPServerConnectionFactory.h>

#include <Common/config.h>

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
    Poco::Logger * log;

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

    std::atomic<size_t> last_connection_id = 0;
public:
    explicit MySQLHandlerFactory(IServer & server_);

    void readRSAKeys();

    void generateRSAKeys();

    Poco::Net::TCPServerConnection * createConnection(const Poco::Net::StreamSocket & socket, TCPServer & tcp_server) override;
};

}
