#pragma once

#include <Poco/MongoDB/Connection.h>


namespace DB
{

class StorageMongoDBSocketFactory : public Poco::MongoDB::Connection::SocketFactory
{
public:
    virtual Poco::Net::StreamSocket createSocket(const std::string & host, int port, Poco::Timespan connectTimeout, bool secure) override;

private:
    static Poco::Net::StreamSocket createPlainSocket(const std::string & host, int port, Poco::Timespan connectTimeout);
    static Poco::Net::StreamSocket createSecureSocket(const std::string & host, int port, Poco::Timespan connectTimeout);
};

}
