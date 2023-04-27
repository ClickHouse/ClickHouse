#include <IO/HTTPCommon.h>

#include <Server/HTTP/HTTPServerResponse.h>
#include <Common/DNSResolver.h>
#include <Common/Exception.h>
#include <Common/PoolBase.h>
#include <Common/ProfileEvents.h>
#include <Common/SipHash.h>

#include "config.h"

#if USE_SSL
#    include <Poco/Net/AcceptCertificateHandler.h>
#    include <Poco/Net/Context.h>
#    include <Poco/Net/HTTPSClientSession.h>
#    include <Poco/Net/InvalidCertificateHandler.h>
#    include <Poco/Net/PrivateKeyPassphraseHandler.h>
#    include <Poco/Net/RejectCertificateHandler.h>
#    include <Poco/Net/SSLManager.h>
#    include <Poco/Net/SecureStreamSocket.h>
#endif

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
        session.setTimeout(timeouts.connection_timeout, timeouts.send_timeout, timeouts.receive_timeout);
        session.setKeepAliveTimeout(timeouts.http_keep_alive_timeout);
    }

    bool isHTTPS(const Poco::URI & uri)
    {
        if (uri.getScheme() == "https")
            return true;
        else if (uri.getScheme() == "http")
            return false;
        else
            throw Exception(ErrorCodes::UNSUPPORTED_URI_SCHEME, "Unsupported scheme in URI '{}'", uri.toString());
    }

    HTTPSessionPtr makeHTTPSessionImpl(const std::string & host, UInt16 port, bool https, bool keep_alive, bool resolve_host = true)
    {
        HTTPSessionPtr session;

        if (https)
        {
#if USE_SSL
            String resolved_host = resolve_host ? DNSResolver::instance().resolveHost(host).toString() : host;
            auto https_session = std::make_shared<Poco::Net::HTTPSClientSession>(host, port);
            if (resolve_host)
                https_session->setResolvedHost(DNSResolver::instance().resolveHost(host).toString());

            session = std::move(https_session);
#else
            throw Exception(ErrorCodes::FEATURE_IS_NOT_ENABLED_AT_BUILD_TIME, "ClickHouse was built without HTTPS support");
#endif
        }
        else
        {
            String resolved_host = resolve_host ? DNSResolver::instance().resolveHost(host).toString() : host;
            session = std::make_shared<Poco::Net::HTTPClientSession>(resolved_host, port);
        }

        ProfileEvents::increment(ProfileEvents::CreatedHTTPConnections);

        session->setKeepAlive(keep_alive);
        return session;
    }

    class SingleEndpointHTTPSessionPool : public PoolBase<Poco::Net::HTTPClientSession>
    {
    private:
        const std::string host;
        const UInt16 port;
        const bool https;
        const String proxy_host;
        const UInt16 proxy_port;
        const bool proxy_https;
        const bool resolve_host;

        using Base = PoolBase<Poco::Net::HTTPClientSession>;

        ObjectPtr allocObject() override
        {
            auto session = makeHTTPSessionImpl(host, port, https, true, resolve_host);
            if (!proxy_host.empty())
            {
                const String proxy_scheme = proxy_https ? "https" : "http";
                session->setProxyHost(proxy_host);
                session->setProxyPort(proxy_port);

                session->setProxyProtocol(proxy_scheme);

                /// Turn on tunnel mode if proxy scheme is HTTP while endpoint scheme is HTTPS.
                session->setProxyTunnel(!proxy_https && https);
            }
            return session;
        }

    public:
        SingleEndpointHTTPSessionPool(
            const std::string & host_,
            UInt16 port_,
            bool https_,
            const std::string & proxy_host_,
            UInt16 proxy_port_,
            bool proxy_https_,
            size_t max_pool_size_,
            bool resolve_host_ = true)
            : Base(static_cast<unsigned>(max_pool_size_), &Poco::Logger::get("HTTPSessionPool"))
            , host(host_)
            , port(port_)
            , https(https_)
            , proxy_host(proxy_host_)
            , proxy_port(proxy_port_)
            , proxy_https(proxy_https_)
            , resolve_host(resolve_host_)
        {
        }
    };

    class HTTPSessionPool : private boost::noncopyable
    {
    public:
        struct Key
        {
            String target_host;
            UInt16 target_port;
            bool is_target_https;
            String proxy_host;
            UInt16 proxy_port;
            bool is_proxy_https;

            bool operator ==(const Key & rhs) const
            {
                return std::tie(target_host, target_port, is_target_https, proxy_host, proxy_port, is_proxy_https)
                    == std::tie(rhs.target_host, rhs.target_port, rhs.is_target_https, rhs.proxy_host, rhs.proxy_port, rhs.is_proxy_https);
            }
        };

    private:
        using PoolPtr = std::shared_ptr<SingleEndpointHTTPSessionPool>;
        using Entry = SingleEndpointHTTPSessionPool::Entry;

        struct Hasher
        {
            size_t operator()(const Key & k) const
            {
                SipHash s;
                s.update(k.target_host);
                s.update(k.target_port);
                s.update(k.is_target_https);
                s.update(k.proxy_host);
                s.update(k.proxy_port);
                s.update(k.is_proxy_https);
                return s.get64();
            }
        };

        std::mutex mutex;
        std::unordered_map<Key, PoolPtr, Hasher> endpoints_pool;

    protected:
        HTTPSessionPool() = default;

    public:
        static auto & instance()
        {
            static HTTPSessionPool instance;
            return instance;
        }

        Entry getSession(
            const Poco::URI & uri,
            const Poco::URI & proxy_uri,
            const ConnectionTimeouts & timeouts,
            size_t max_connections_per_endpoint,
            bool resolve_host = true)
        {
            std::lock_guard lock(mutex);
            const std::string & host = uri.getHost();
            UInt16 port = uri.getPort();
            bool https = isHTTPS(uri);


            String proxy_host;
            UInt16 proxy_port = 0;
            bool proxy_https = false;
            if (!proxy_uri.empty())
            {
                proxy_host = proxy_uri.getHost();
                proxy_port = proxy_uri.getPort();
                proxy_https = isHTTPS(proxy_uri);
            }

            HTTPSessionPool::Key key{host, port, https, proxy_host, proxy_port, proxy_https};
            auto pool_ptr = endpoints_pool.find(key);
            if (pool_ptr == endpoints_pool.end())
                std::tie(pool_ptr, std::ignore) = endpoints_pool.emplace(
                    key, std::make_shared<SingleEndpointHTTPSessionPool>(host, port, https, proxy_host, proxy_port, proxy_https, max_connections_per_endpoint, resolve_host));

            auto retry_timeout = timeouts.connection_timeout.totalMicroseconds();
            auto session = pool_ptr->second->get(retry_timeout);

            /// We store session reusability state in session data.
            /// Poco HTTPSession also stores exception, but it can be removed at any time.
            const auto & session_data = session->sessionData();
            if (!Poco::AnyCast<HTTPSessionReusableTag>(&session_data))
            {
                /// The session may be in a non-reusable state, e.g. in the middle of receiving
                /// response to a previous request. Close the connection.
                session->reset();

                if (resolve_host)
                {
                    /// Host can change IP
                    const auto ip = DNSResolver::instance().resolveHost(host).toString();
                    if (ip != session->getHost())
                        session->setHost(ip);
                }
            }

            session->attachSessionData({});

            setTimeouts(*session, timeouts);

            return session;
        }
    };
}

void setResponseDefaultHeaders(HTTPServerResponse & response, size_t keep_alive_timeout)
{
    if (!response.getKeepAlive())
        return;

    Poco::Timespan timeout(keep_alive_timeout, 0);
    if (timeout.totalSeconds())
        response.set("Keep-Alive", "timeout=" + std::to_string(timeout.totalSeconds()));
}

HTTPSessionPtr makeHTTPSession(const Poco::URI & uri, const ConnectionTimeouts & timeouts, bool resolve_host)
{
    const std::string & host = uri.getHost();
    UInt16 port = uri.getPort();
    bool https = isHTTPS(uri);

    auto session = makeHTTPSessionImpl(host, port, https, false, resolve_host);
    setTimeouts(*session, timeouts);
    return session;
}


PooledHTTPSessionPtr makePooledHTTPSession(const Poco::URI & uri, const ConnectionTimeouts & timeouts, size_t per_endpoint_pool_size, bool resolve_host)
{
    return makePooledHTTPSession(uri, {}, timeouts, per_endpoint_pool_size, resolve_host);
}

PooledHTTPSessionPtr makePooledHTTPSession(const Poco::URI & uri, const Poco::URI & proxy_uri, const ConnectionTimeouts & timeouts, size_t per_endpoint_pool_size, bool resolve_host)
{
    return HTTPSessionPool::instance().getSession(uri, proxy_uri, timeouts, per_endpoint_pool_size, resolve_host);
}

bool isRedirect(const Poco::Net::HTTPResponse::HTTPStatus status) { return status == Poco::Net::HTTPResponse::HTTP_MOVED_PERMANENTLY  || status == Poco::Net::HTTPResponse::HTTP_FOUND || status == Poco::Net::HTTPResponse::HTTP_SEE_OTHER  || status == Poco::Net::HTTPResponse::HTTP_TEMPORARY_REDIRECT; }

std::istream * receiveResponse(
    Poco::Net::HTTPClientSession & session, const Poco::Net::HTTPRequest & request, Poco::Net::HTTPResponse & response, const bool allow_redirects)
{
    auto & istr = session.receiveResponse(response);
    assertResponseIsOk(request, response, istr, allow_redirects);
    return &istr;
}

void assertResponseIsOk(const Poco::Net::HTTPRequest & request, Poco::Net::HTTPResponse & response, std::istream & istr, const bool allow_redirects)
{
    auto status = response.getStatus();

    if (!(status == Poco::Net::HTTPResponse::HTTP_OK
        || status == Poco::Net::HTTPResponse::HTTP_CREATED
        || status == Poco::Net::HTTPResponse::HTTP_ACCEPTED
        || status == Poco::Net::HTTPResponse::HTTP_PARTIAL_CONTENT /// Reading with Range header was successful.
        || (isRedirect(status) && allow_redirects)))
    {
        int code = status == Poco::Net::HTTPResponse::HTTP_TOO_MANY_REQUESTS
            ? ErrorCodes::RECEIVED_ERROR_TOO_MANY_REQUESTS
            : ErrorCodes::RECEIVED_ERROR_FROM_REMOTE_IO_SERVER;

        std::stringstream body; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        body.exceptions(std::ios::failbit);
        body << istr.rdbuf();

        throw HTTPException(code, request.getURI(), status, response.getReason(), body.str());
    }
}

Exception HTTPException::makeExceptionMessage(
    int code,
    const std::string & uri,
    Poco::Net::HTTPResponse::HTTPStatus http_status,
    const std::string & reason,
    const std::string & body)
{
    return Exception(code,
        "Received error from remote server {}. "
        "HTTP status code: {} {}, "
        "body: {}",
        uri, static_cast<int>(http_status), reason, body);
}

}
