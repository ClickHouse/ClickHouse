#pragma once

#include <Poco/Net/TCPServerConnectionFactory.h>
#include <atomic>
#include <openssl/rsa.h>
#include "IServer.h"

namespace DB
{

class MySQLHandlerFactory : public Poco::Net::TCPServerConnectionFactory
{
private:
    IServer & server;
    Poco::Logger * log;

    struct RSADeleter
    {
        void operator()(RSA * ptr) { RSA_free(ptr); }
    };
    using RSAPtr = std::unique_ptr<RSA, RSADeleter>;

    RSAPtr public_key;
    RSAPtr private_key;

    bool ssl_enabled = true;

    std::atomic<size_t> last_connection_id = 0;
public:
    explicit MySQLHandlerFactory(IServer & server_);

    void readRSAKeys();

    void generateRSAKeys();

    Poco::Net::TCPServerConnection * createConnection(const Poco::Net::StreamSocket & socket) override;
};

}
