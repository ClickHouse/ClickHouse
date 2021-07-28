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
    return secure ? createSecureSocket(host, port, connectTimeout) : createPlainSocket(host, port, connectTimeout);
}

Poco::Net::StreamSocket StorageMongoDBSocketFactory::createPlainSocket(const std::string & host, int port, Poco::Timespan connectTimeout)
{
    Poco::Net::SocketAddress address(host, port);
    Poco::Net::StreamSocket socket;

    socket.connect(address, connectTimeout);

    return socket;
}


Poco::Net::StreamSocket StorageMongoDBSocketFactory::createSecureSocket(const std::string & host, int port, Poco::Timespan connectTimeout)
{
#if USE_SSL
    Poco::Net::SocketAddress address(host, port);
    Poco::Net::SecureStreamSocket socket;

    socket.connect(address, connectTimeout);

    return socket;
#else
    return createPlainSocket(host, port, connectTimeout);
#endif
}

}
