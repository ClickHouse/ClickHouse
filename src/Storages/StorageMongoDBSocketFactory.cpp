#include "StorageMongoDBSocketFactory.h"

#include <Common/Exception.h>

#include <Common/config.h>

#include <Poco/Net/IPAddress.h>
#include <Poco/Net/SocketAddress.h>

#if USE_SSL
#   include <Poco/Net/SecureStreamSocket.h>
#endif


namespace DB
{

namespace ErrorCodes
{
    extern const int FEATURE_IS_NOT_ENABLED_AT_BUILD_TIME;
}

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


Poco::Net::StreamSocket StorageMongoDBSocketFactory::createSecureSocket(const std::string & host [[maybe_unused]], int port [[maybe_unused]], Poco::Timespan connectTimeout [[maybe_unused]])
{
#if USE_SSL
    Poco::Net::SocketAddress address(host, port);
    Poco::Net::SecureStreamSocket socket;

    socket.connect(address, connectTimeout);

    return socket;
#else
    throw Exception("SSL is not enabled at build time.", ErrorCodes::FEATURE_IS_NOT_ENABLED_AT_BUILD_TIME);
#endif
}

}
