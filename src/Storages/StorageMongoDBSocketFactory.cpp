#include "StorageMongoDBSocketFactory.h"

#include <Poco/Net/IPAddress.h>
#include <Poco/Net/SocketAddress.h>

#if USE_SSL
#   include <Poco/Net/SecureStreamSocket.h>
#endif

#pragma clang diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wunused-parameter"


namespace DB
{

Poco::Net::StreamSocket StorageMongoDBSocketFactory::createSocket(const std::string & host, int port, Poco::Timespan connectTimeout, bool secure)
{
#if USE_SSL
    return secure ? createSecureSocket(host, port) : createPlainSocket(host, port);
#else
    return createPlainSocket(host, port);
#endif
}

Poco::Net::StreamSocket StorageMongoDBSocketFactory::createPlainSocket(const std::string & host, int port)
{
    Poco::Net::SocketAddress address(host, port);
    Poco::Net::StreamSocket socket(address);

    return socket;
}

#if USE_SSL
Poco::Net::StreamSocket StorageMongoDBSocketFactory::createSecureSocket(const std::string & host, int port)
{
    Poco::Net::SocketAddress address(host, port);
    Poco::Net::SecureStreamSocket socket(address, host);

    return socket;
}
#endif

}
