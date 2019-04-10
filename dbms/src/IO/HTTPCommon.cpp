#include <IO/HTTPCommon.h>

#include <Common/config.h>
#include <Common/DNSResolver.h>
#include <Common/Exception.h>
#include <Common/PoolBase.h>
#include <Common/ProfileEvents.h>
#include <Common/SipHash.h>

#include <Poco/Version.h>

#if USE_POCO_NETSSL
#include <Poco/Net/AcceptCertificateHandler.h>
#include <Poco/Net/Context.h>
#include <Poco/Net/HTTPSClientSession.h>
#include <Poco/Net/InvalidCertificateHandler.h>
#include <Poco/Net/PrivateKeyPassphraseHandler.h>
#include <Poco/Net/RejectCertificateHandler.h>
#include <Poco/Net/SSLManager.h>
#endif

#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/Util/Application.h>

#include <tuple>
#include <unordered_map>
#include <sstream>


namespace ProfileEvents
{
    extern const Event CreatedHTTPConnections;
}

namespace DB
{
namespace ErrorCodes
{
    extern const int RECEIVED_ERROR_FROM_REMOTE_IO_SERVER;
    extern const int RECEIVED_ERROR_TOO_MANY_REQUESTS;
    extern const int FEATURE_IS_NOT_ENABLED_AT_BUILD_TIME;
    extern const int UNSUPPORTED_URI_SCHEME;
}


namespace
{
void setTimeouts(Poco::Net::HTTPClientSession & session, const ConnectionTimeouts & timeouts)
    {
#if defined(POCO_CLICKHOUSE_PATCH) || POCO_VERSION >= 0x02000000
        session.setTimeout(timeouts.connection_timeout, timeouts.send_timeout, timeouts.receive_timeout);
#else
        session.setTimeout(std::max({timeouts.connection_timeout, timeouts.send_timeout, timeouts.receive_timeout}));
#endif
        session.setKeepAliveTimeout(timeouts.http_keep_alive_timeout);
    }

    bool isHTTPS(const Poco::URI & uri)
    {
        if (uri.getScheme() == "https")
            return true;
        else if (uri.getScheme() == "http")
            return false;
        else
            throw Exception("Unsupported scheme in URI '" + uri.toString() + "'", ErrorCodes::UNSUPPORTED_URI_SCHEME);
    }

    HTTPSessionPtr makeHTTPSessionImpl(const std::string & host, UInt16 port, bool https, bool keep_alive)
    {
        HTTPSessionPtr session;

        if (https)
#if USE_POCO_NETSSL
            session = std::make_shared<Poco::Net::HTTPSClientSession>();
#else
            throw Exception("ClickHouse was built without HTTPS support", ErrorCodes::FEATURE_IS_NOT_ENABLED_AT_BUILD_TIME);
#endif
        else
            session = std::make_shared<Poco::Net::HTTPClientSession>();

        ProfileEvents::increment(ProfileEvents::CreatedHTTPConnections);

        session->setHost(DNSResolver::instance().resolveHost(host).toString());
        session->setPort(port);

        /// doesn't work properly without patch
#if defined(POCO_CLICKHOUSE_PATCH)
        session->setKeepAlive(keep_alive);
#else
        (void)keep_alive; // Avoid warning: unused parameter
#endif

        return session;
    }

    class SingleEndpointHTTPSessionPool : public PoolBase<Poco::Net::HTTPClientSession>
    {
    private:
        const std::string host;
        const UInt16 port;
        bool https;
        using Base = PoolBase<Poco::Net::HTTPClientSession>;
        ObjectPtr allocObject() override
        {
            return makeHTTPSessionImpl(host, port, https, true);
        }

    public:
        SingleEndpointHTTPSessionPool(const std::string & host_, UInt16 port_, bool https_, size_t max_pool_size_)
            : Base(max_pool_size_, &Poco::Logger::get("HTTPSessionPool")), host(host_), port(port_), https(https_)
        {
        }
    };

    class HTTPSessionPool : public ext::singleton<HTTPSessionPool>
    {
    private:
        using Key = std::tuple<std::string, UInt16, bool>;
        using PoolPtr = std::shared_ptr<SingleEndpointHTTPSessionPool>;
        using Entry = SingleEndpointHTTPSessionPool::Entry;

        friend class ext::singleton<HTTPSessionPool>;

        struct Hasher
        {
            size_t operator()(const Key & k) const
            {
                SipHash s;
                s.update(std::get<0>(k));
                s.update(std::get<1>(k));
                s.update(std::get<2>(k));
                return s.get64();
            }
        };

        std::mutex mutex;
        std::unordered_map<Key, PoolPtr, Hasher> endpoints_pool;

    protected:
        HTTPSessionPool() = default;

    public:
        Entry getSession(
            const Poco::URI & uri,
            const ConnectionTimeouts & timeouts,
            size_t max_connections_per_endpoint)
        {
            std::unique_lock lock(mutex);
            const std::string & host = uri.getHost();
            UInt16 port = uri.getPort();
            bool https = isHTTPS(uri);
            auto key = std::make_tuple(host, port, https);
            auto pool_ptr = endpoints_pool.find(key);
            if (pool_ptr == endpoints_pool.end())
                std::tie(pool_ptr, std::ignore) = endpoints_pool.emplace(
                    key, std::make_shared<SingleEndpointHTTPSessionPool>(host, port, https, max_connections_per_endpoint));

            auto retry_timeout = timeouts.connection_timeout.totalMicroseconds();
            auto session = pool_ptr->second->get(retry_timeout);

            setTimeouts(*session, timeouts);

            return session;
        }
    };
}

void setResponseDefaultHeaders(Poco::Net::HTTPServerResponse & response, unsigned keep_alive_timeout)
{
    if (!response.getKeepAlive())
        return;

    Poco::Timespan timeout(keep_alive_timeout, 0);
    if (timeout.totalSeconds())
        response.set("Keep-Alive", "timeout=" + std::to_string(timeout.totalSeconds()));
}

HTTPSessionPtr makeHTTPSession(const Poco::URI & uri, const ConnectionTimeouts & timeouts)
{
    const std::string & host = uri.getHost();
    UInt16 port = uri.getPort();
    bool https = isHTTPS(uri);

    auto session = makeHTTPSessionImpl(host, port, https, false);
    setTimeouts(*session, timeouts);
    return session;
}


PooledHTTPSessionPtr makePooledHTTPSession(const Poco::URI & uri, const ConnectionTimeouts & timeouts, size_t per_endpoint_pool_size)
{
    return HTTPSessionPool::instance().getSession(uri, timeouts, per_endpoint_pool_size);
}


std::istream * receiveResponse(
    Poco::Net::HTTPClientSession & session, const Poco::Net::HTTPRequest & request, Poco::Net::HTTPResponse & response)
{
    auto istr = &session.receiveResponse(response);
    auto status = response.getStatus();

    if (status != Poco::Net::HTTPResponse::HTTP_OK)
    {
        std::stringstream error_message;
        error_message << "Received error from remote server " << request.getURI() << ". HTTP status code: " << status << " "
                      << response.getReason() << ", body: " << istr->rdbuf();

        throw Exception(error_message.str(),
            status == HTTP_TOO_MANY_REQUESTS ? ErrorCodes::RECEIVED_ERROR_TOO_MANY_REQUESTS
                                             : ErrorCodes::RECEIVED_ERROR_FROM_REMOTE_IO_SERVER);
    }
    return istr;
}

}
