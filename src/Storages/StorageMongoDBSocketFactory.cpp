#include "StorageMongoDBSocketFactory.h"

#if !defined(ARCADIA_BUILD)
#   include <Common/config.h>
#endif

#include <Poco/Net/IPAddress.h>
#include <Poco/Net/SocketAddress.h>

#if USE_SSL
#   include <Poco/Net/SecureStreamSocket.h>
#endif


namespace DB
{

Poco::Net::StreamSocket StorageMongoDBSocketFactory::createSocket(const std::string & host, int port, Poco::Timespan connectTimeout, bool secure)
{
#if USE_SSL
    return secure ? createSecureSocket(host, port, connectTimeout) : createPlainSocket(host, port, connectTimeout);
#else
    return createPlainSocket(host, port, connectTimeout);
#endif
}

Poco::Net::StreamSocket StorageMongoDBSocketFactory::createPlainSocket(const std::string & host, int port, Poco::Timespan connectTimeout)
{
    Poco::Net::SocketAddress address(host, port);
    Poco::Net::StreamSocket socket;

    socket.connect(address, connectTimeout);

    return socket;
}

#if USE_SSL
Poco::Net::StreamSocket StorageMongoDBSocketFactory::createSecureSocket(const std::string & host, int port, Poco::Timespan connectTimeout)
{
    Poco::Net::SocketAddress address(host, port);
    Poco::Net::SecureStreamSocket socket;

    socket.connect(address, connectTimeout);

    return socket;
}
#endif

}
