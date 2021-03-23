#include "StorageMongoDBSocketFactory.h"

#include <Poco/Net/IPAddress.h>
#include <Poco/Net/SocketAddress.h>
#include <Poco/Net/SecureStreamSocket.h>


#pragma clang diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wunused-parameter"


namespace DB
{

Poco::Net::StreamSocket StorageMongoDBSocketFactory::createSocket(const std::string & host, int port, Poco::Timespan connectTimeout, bool secure)
{
    return secure ? createSecureSocket(host, port) : createPlainSocket(host, port);
}

Poco::Net::StreamSocket StorageMongoDBSocketFactory::createPlainSocket(const std::string & host, int port)
{
    Poco::Net::SocketAddress address(host, port);
    Poco::Net::StreamSocket socket(address);

    return socket;
}

Poco::Net::StreamSocket StorageMongoDBSocketFactory::createSecureSocket(const std::string & host, int port)
{
    Poco::Net::SocketAddress address(host, port);
    Poco::Net::SecureStreamSocket socket(address, host);

    return socket;
}

}
