#pragma once
#include <Poco/Net/IPAddress.h>
#include <Poco/Net/SocketAddress.h>
#include <memory>
#include <ext/singleton.h>


namespace DB
{

class DNSCache : public ext::singleton<DNSCache>
{
public:

    DNSCache(const DNSCache &) = delete;

    Poco::Net::IPAddress resolveHost(const std::string & host);

    Poco::Net::SocketAddress resolveHostAndPort(const std::string & host_and_port);

    void dropCache();

    ~DNSCache();

protected:

    DNSCache();

    friend class ext::singleton<DNSCache>;

    struct Impl;
    std::unique_ptr<Impl> impl;
};

}
