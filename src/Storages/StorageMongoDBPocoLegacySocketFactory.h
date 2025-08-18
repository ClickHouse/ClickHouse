#pragma once

#include "config.h"

#if USE_MONGODB
#include <Poco/MongoDB/Connection.h>


namespace DB
{

/// Deprecated, will be removed soon.
class StorageMongoDBPocoLegacySocketFactory : public Poco::MongoDB::Connection::SocketFactory
{
public:
    Poco::Net::StreamSocket createSocket(const std::string & host, int port, Poco::Timespan connectTimeout, bool secure) override;

private:
    static Poco::Net::StreamSocket createPlainSocket(const std::string & host, int port, Poco::Timespan connectTimeout);
    static Poco::Net::StreamSocket createSecureSocket(const std::string & host, int port, Poco::Timespan connectTimeout);
};

}
#endif
