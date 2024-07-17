//
// HTTPClientSession.h
//
// Library: Net
// Package: HTTPClient
// Module:  HTTPClientSession
//
// Definition of the HTTPClientSession class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Net_HTTPClientSession_INCLUDED
#define Net_HTTPClientSession_INCLUDED


#include <istream>
#include <ostream>
#include "Poco/Net/HTTPSession.h"
#include "Poco/Net/HTTPSessionFactory.h"
#include "Poco/Net/Net.h"
#include "Poco/Net/SocketAddress.h"
#include "Poco/SharedPtr.h"


namespace Poco
{
namespace Net
{


    class HTTPRequest;
    class HTTPResponse;


    class Net_API HTTPClientSession : public HTTPSession
    /// This class implements the client-side of
    /// a HTTP session.
    ///
    /// To send a HTTP request to a HTTP server, first
    /// instantiate a HTTPClientSession object and
    /// specify the server's host name and port number.
    ///
    /// Then create a HTTPRequest object, fill it accordingly,
    /// and pass it as argument to the sendRequest() method.
    ///
    /// sendRequest() will return an output stream that can
    /// be used to send the request body, if there is any.
    ///
    /// After you are done sending the request body, create
    /// a HTTPResponse object and pass it to receiveResponse().
    ///
    /// This will return an input stream that can be used to
    /// read the response body.
    ///
    /// See RFC 2616 <http://www.faqs.org/rfcs/rfc2616.html> for more
    /// information about the HTTP protocol.
    ///
    /// Proxies and proxy authorization (only HTTP Basic Authorization)
    /// is supported. Use setProxy() and setProxyCredentials() to
    /// set up a session through a proxy.
    {
    public:
        struct ProxyConfig
        /// HTTP proxy server configuration.
        {
            ProxyConfig() : port(HTTP_PORT), protocol("http"), tunnel(true), originalRequestProtocol("http") { }

            std::string host;
            /// Proxy server host name or IP address.
            Poco::UInt16 port;
            /// Proxy server TCP port.
            std::string protocol;
            /// Protocol to use (http or https).
            bool tunnel;
            /// Use proxy as tunnel (establish 2-way communication through CONNECT request).
            /// If tunnel option is 'false' request will be send directly to proxy without CONNECT request.
            std::string username;
            /// Proxy server username.
            std::string password;
            /// Proxy server password.
            std::string nonProxyHosts;
            /// A regular expression defining hosts for which the proxy should be bypassed,
            /// e.g. "localhost|127\.0\.0\.1|192\.168\.0\.\d+". Can also be an empty
            /// string to disable proxy bypassing.
            std::string originalRequestProtocol;
            /// Original request protocol (http or https).
            /// Required in the case of: HTTPS request over HTTP proxy with tunneling (CONNECT) off.
        };

        HTTPClientSession();
        /// Creates an unconnected HTTPClientSession.

        explicit HTTPClientSession(const StreamSocket & socket);
        /// Creates a HTTPClientSession using the given socket.
        /// The socket must not be connected. The session
        /// takes ownership of the socket.

        explicit HTTPClientSession(const SocketAddress & address);
        /// Creates a HTTPClientSession using the given address.

        HTTPClientSession(const std::string & host, Poco::UInt16 port = HTTPSession::HTTP_PORT);
        /// Creates a HTTPClientSession using the given host and port.

        HTTPClientSession(const std::string & host, Poco::UInt16 port, const ProxyConfig & proxyConfig);
        /// Creates a HTTPClientSession using the given host, port and proxy configuration.

        virtual ~HTTPClientSession();
        /// Destroys the HTTPClientSession and closes
        /// the underlying socket.

        void setHost(const std::string & host);
        /// Sets the host name of the target HTTP server.
        ///
        /// The host must not be changed once there is an
        /// open connection to the server.

        const std::string & getHost() const;
        /// Returns the host name of the target HTTP server.

        void setPort(Poco::UInt16 port);
        /// Sets the port number of the target HTTP server.
        ///
        /// The port number must not be changed once there is an
        /// open connection to the server.

        void setResolvedHost(std::string resolved_host) { _resolved_host.swap(resolved_host); }

        std::string getResolvedHost() const { return _resolved_host; }
        /// Returns the resolved IP address of the target HTTP server.

        Poco::UInt16 getPort() const;
        /// Returns the port number of the target HTTP server.

        std::string getResolvedAddress() const;
        /// Returns the resolved host name and port of the target HTTP server.

        void setProxy(
            const std::string & host,
            Poco::UInt16 port = HTTPSession::HTTP_PORT,
            const std::string & protocol = "http",
            bool tunnel = true);
        /// Sets the proxy host name, port number, protocol (http or https) and tunnel behaviour.

        void setProxyHost(const std::string & host);
        /// Sets the host name of the proxy server.

        void setProxyPort(Poco::UInt16 port);
        /// Sets the port number of the proxy server.

        void setProxyProtocol(const std::string & protocol);
        /// Sets the proxy protocol (http or https).

        void setProxyTunnel(bool tunnel);
        /// If 'true' proxy will be used as tunnel.

        const std::string & getProxyHost() const;
        /// Returns the proxy host name.

        Poco::UInt16 getProxyPort() const;
        /// Returns the proxy port number.

        const std::string & getProxyProtocol() const;
        /// Returns the proxy protocol.

        bool isProxyTunnel() const;
        /// Returns 'true' if proxy is configured as tunnel.

        void setProxyCredentials(const std::string & username, const std::string & password);
        /// Sets the username and password for proxy authentication.
        /// Only Basic authentication is supported.

        void setProxyUsername(const std::string & username);
        /// Sets the username for proxy authentication.
        /// Only Basic authentication is supported.

        const std::string & getProxyUsername() const;
        /// Returns the username for proxy authentication.

        void setProxyPassword(const std::string & password);
        /// Sets the password for proxy authentication.
        /// Only Basic authentication is supported.

        const std::string & getProxyPassword() const;
        /// Returns the password for proxy authentication.

        void setProxyConfig(const ProxyConfig & config);
        /// Sets the proxy configuration.

        const ProxyConfig & getProxyConfig() const;
        /// Returns the proxy configuration.

        static void setGlobalProxyConfig(const ProxyConfig & config);
        /// Sets the global proxy configuration.
        ///
        /// The global proxy configuration is used by all HTTPClientSession
        /// instances, unless a different proxy configuration is explicitly set.
        ///
        /// Warning: Setting the global proxy configuration is not thread safe.
        /// The global proxy configuration should be set at start up, before
        /// the first HTTPClientSession instance is created.

        static const ProxyConfig & getGlobalProxyConfig();
        /// Returns the global proxy configuration.

        void setKeepAliveTimeout(const Poco::Timespan & timeout);
        /// Sets the connection timeout for HTTP connections.

        Poco::Timespan getKeepAliveTimeout() const;
        /// Returns the connection timeout for HTTP connections.

        void setKeepAliveMaxRequests(int max_requests);

        int getKeepAliveMaxRequests() const;

        int getKeepAliveRequest() const;

        bool isKeepAliveExpired(double reliability = 1.0) const;
        /// Returns if the connection is expired with some margin as fraction of timeout as reliability

        double getKeepAliveReliability() const;
        /// Returns the current fraction of keep alive timeout when connection is considered safe to use
        /// It helps to avoid situation when a client uses nearly expired connection and receives NoMessageException

        virtual std::ostream & sendRequest(HTTPRequest & request);
        /// Sends the header for the given HTTP request to
        /// the server.
        ///
        /// The HTTPClientSession will set the request's
        /// Host and Keep-Alive headers accordingly.
        ///
        /// The returned output stream can be used to write
        /// the request body. The stream is valid until
        /// receiveResponse() is called or the session
        /// is destroyed.
        ///
        /// In case a network or server failure happens
        /// while writing the request body to the returned stream,
        /// the stream state will change to bad or fail. In this
        /// case, reset() should be called if the session will
        /// be reused and persistent connections are enabled
        /// to ensure a new connection will be set up
        /// for the next request.

        virtual std::istream & receiveResponse(HTTPResponse & response);
        /// Receives the header for the response to the previous
        /// HTTP request.
        ///
        /// The returned input stream can be used to read
        /// the response body. The stream is valid until
        /// sendRequest() is called or the session is
        /// destroyed.
        ///
        /// It must be ensured that the response stream
        /// is fully consumed before sending a new request
        /// and persistent connections are enabled. Otherwise,
        /// the unread part of the response body may be treated as
        /// part of the next request's response header, resulting
        /// in a Poco::Net::MessageException being thrown.
        ///
        /// In case a network or server failure happens
        /// while reading the response body from the returned stream,
        /// the stream state will change to bad or fail. In this
        /// case, reset() should be called if the session will
        /// be reused and persistent connections are enabled
        /// to ensure a new connection will be set up
        /// for the next request.

        virtual bool peekResponse(HTTPResponse & response);
        /// If the request contains a "Expect: 100-continue" header,
        /// (see HTTPRequest::setExpectContinue()) this method can be
        /// used to check whether the server has sent a 100 Continue response
        /// before continuing with the request, i.e. sending the request body,
        /// after calling sendRequest().
        ///
        /// Returns true if the server has responded with 100 Continue,
        /// otherwise false. The HTTPResponse object contains the
        /// response sent by the server.
        ///
        /// In any case, receiveResponse() must be called afterwards as well in
        /// order to complete the request. The same HTTPResponse object
        /// passed to peekResponse() must also be passed to receiveResponse().
        ///
        /// This method should only be called if the request contains
        /// a "Expect: 100-continue" header.

        virtual void flushRequest();
        /// Flushes the request stream.
        ///
        /// Normally this method does not need to be called.
        /// It can be used to ensure the request has been
        /// fully sent if receiveResponse() is not called, e.g.,
        /// because the underlying socket will be detached.

        virtual void reset();
        /// Resets the session and closes the socket.
        ///
        /// The next request will initiate a new connection,
        /// even if persistent connections are enabled.
        ///
        /// This should be called whenever something went
        /// wrong when sending a request (e.g., sendRequest()
        /// or receiveResponse() throws an exception, or
        /// the request or response stream changes into
        /// fail or bad state, but not eof state).

        virtual bool secure() const;
        /// Return true iff the session uses SSL or TLS,
        /// or false otherwise.

        bool bypassProxy() const;
        /// Returns true if the proxy should be bypassed
        /// for the current host.

        const Poco::Timestamp & getLastRequest() const;
        /// Returns time when connection has been used last time

    protected:
        enum
        {
            DEFAULT_KEEP_ALIVE_TIMEOUT = 8
        };

        virtual void reconnect();
        /// Connects the underlying socket to the HTTP server.

        int write(const char * buffer, std::streamsize length);
        /// Tries to re-connect if keep-alive is on.

        virtual std::string proxyRequestPrefix() const;
        /// Returns the prefix prepended to the URI for proxy requests
        /// (e.g., "http://myhost.com").

        virtual bool mustReconnect() const;
        /// Checks if we can reuse a persistent connection.

        virtual void proxyAuthenticate(HTTPRequest & request);
        /// Sets the proxy credentials (Proxy-Authorization header), if
        /// proxy username and password have been set.

        void proxyAuthenticateImpl(HTTPRequest & request);
        /// Sets the proxy credentials (Proxy-Authorization header), if
        /// proxy username and password have been set.

        StreamSocket proxyConnect();
        /// Sends a CONNECT request to the proxy server and returns
        /// a StreamSocket for the resulting connection.

        void proxyTunnel();
        /// Calls proxyConnect() and attaches the resulting StreamSocket
        /// to the HTTPClientSession.

        void setLastRequest(Poco::Timestamp time);

        void assign(HTTPClientSession & session);

        void setKeepAliveRequest(int request);

        HTTPSessionFactory _proxySessionFactory;
        /// Factory to create HTTPClientSession to proxy.
    private:
        std::string _host;
        std::string _resolved_host;
        Poco::UInt16 _port;
        ProxyConfig _proxyConfig;
        Poco::Timespan _keepAliveTimeout;
        int _keepAliveCurrentRequest = 0;
        int _keepAliveMaxRequests = 1000;
        Poco::Timestamp _lastRequest;
        bool _reconnect;
        bool _mustReconnect;
        bool _expectResponseBody;
        bool _responseReceived;
        Poco::SharedPtr<std::ostream> _pRequestStream;
        Poco::SharedPtr<std::istream> _pResponseStream;

        static const double _defaultKeepAliveReliabilityLevel;
        static ProxyConfig _globalProxyConfig;

        HTTPClientSession(const HTTPClientSession &);
        HTTPClientSession & operator=(const HTTPClientSession &);

        friend class WebSocket;
    };


    //
    // inlines
    //

    inline std::string HTTPClientSession::getResolvedAddress() const
    {
        return (_resolved_host.empty() ? _host : _resolved_host) + ':' + std::to_string(_port);
    }


    inline const std::string & HTTPClientSession::getHost() const
    {
        return _host;
    }


    inline Poco::UInt16 HTTPClientSession::getPort() const
    {
        return _port;
    }


    inline const std::string & HTTPClientSession::getProxyHost() const
    {
        return _proxyConfig.host;
    }


    inline Poco::UInt16 HTTPClientSession::getProxyPort() const
    {
        return _proxyConfig.port;
    }


    inline const std::string & HTTPClientSession::getProxyProtocol() const
    {
        return _proxyConfig.protocol;
    }


    inline bool HTTPClientSession::isProxyTunnel() const
    {
        return _proxyConfig.tunnel;
    }


    inline const std::string & HTTPClientSession::getProxyUsername() const
    {
        return _proxyConfig.username;
    }


    inline const std::string & HTTPClientSession::getProxyPassword() const
    {
        return _proxyConfig.password;
    }


    inline const HTTPClientSession::ProxyConfig & HTTPClientSession::getProxyConfig() const
    {
        return _proxyConfig;
    }


    inline const HTTPClientSession::ProxyConfig & HTTPClientSession::getGlobalProxyConfig()
    {
        return _globalProxyConfig;
    }


    inline Poco::Timespan HTTPClientSession::getKeepAliveTimeout() const
    {
        return _keepAliveTimeout;
    }

    inline const Poco::Timestamp & HTTPClientSession::getLastRequest() const
    {
        return _lastRequest;
    }

    inline double HTTPClientSession::getKeepAliveReliability() const
    {
        return _defaultKeepAliveReliabilityLevel;
    }

    inline int HTTPClientSession::getKeepAliveMaxRequests() const
    {
        return _keepAliveMaxRequests;
    }

    inline int HTTPClientSession::getKeepAliveRequest() const
    {
        return _keepAliveCurrentRequest;
    }

}
} // namespace Poco::Net


#endif // Net_HTTPClientSession_INCLUDED
