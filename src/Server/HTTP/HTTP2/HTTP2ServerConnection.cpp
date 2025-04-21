#include <Common/Logger.h>
#include <Common/logger_useful.h>

#include <Server/HTTP/HTTP2/HTTP2ServerConnection.h>
#include <Server/HTTP/HTTP2/HTTP2ServerParams.h>

#include <Poco/Net/SecureServerSocket.h>
#include <Poco/Net/SecureServerSocketImpl.h>

namespace DB
{

static const std::string HTTP2_ALPN = "h2";

static unsigned char ALPN_PROTOCOLS[] = { 2, 'h', '2',};

bool setHTTP2Alpn(const Poco::Net::SecureServerSocket & socket, HTTP2ServerParams::Ptr http2_params)
{
    if (!http2_params)
        return false;
    Poco::Net::Context::Ptr context = socket.context();
    if (!context)
        return false;
    SSL_CTX * ssl_ctx = context->sslContext();
    if (!ssl_ctx)
        return false;

    SSL_CTX_set_alpn_protos(ssl_ctx, ALPN_PROTOCOLS, sizeof(ALPN_PROTOCOLS));

    auto alpn_select_cb = [](
        SSL* /*ssl*/,
        const unsigned char** out,
        unsigned char* outlen,
        const unsigned char* in,
        unsigned int inlen,
        void* /*arg*/) -> int
    {
        int res = SSL_select_next_proto(
            const_cast<unsigned char**>(out),
            outlen,
            ALPN_PROTOCOLS,
            sizeof(ALPN_PROTOCOLS), 
            in, inlen);
        if (res == OPENSSL_NPN_NEGOTIATED)
            return SSL_TLSEXT_ERR_OK;
        return SSL_TLSEXT_ERR_NOACK;
    };
    SSL_CTX_set_alpn_select_cb(ssl_ctx, std::move(alpn_select_cb), nullptr);
    return true;
}

/// TODO:
/// Currently we only look at ALPN to determine the HTTP version
/// It would be nice to support HTTP/2 prior knowledge mechanism for non-TLS connections
/// But it will require to read some data from the socket (to check HTTP/2 preface)
/// So we will need to make HTTPServerConnection work with some buffer insted of raw socket
/// So it can process already received data
bool isHTTP2Connection(const Poco::Net::StreamSocket & socket, HTTP2ServerParams::Ptr http2_params)
{
    if (!http2_params)
        return false;
    if (!socket.secure())
        return false;
    // dynamic_cast looks like a hack but can't think of a better way
    const Poco::Net::SecureServerSocketImpl * ssocket = dynamic_cast<const Poco::Net::SecureServerSocketImpl *>(socket.impl());
    chassert(ssocket != nullptr);  // If this happens then somethinh has changed in Poco internals
    if (ssocket == nullptr)
        return false;
    std::string alpn_selected = ssocket->getAlpnSelected();
    return alpn_selected == HTTP2_ALPN;
}

HTTP2ServerConnection::HTTP2ServerConnection(
    HTTPContextPtr context_,
    TCPServer & tcp_server_,
    const Poco::Net::StreamSocket & socket_,
    HTTP2ServerParams::Ptr params_,
    HTTPRequestHandlerFactoryPtr factory_,
    const ProfileEvents::Event & read_event_,
    const ProfileEvents::Event & write_event_)
    : TCPServerConnection(socket_), context(std::move(context_)), tcp_server(tcp_server_), params(params_), factory(factory_), read_event(read_event_), write_event(write_event_), stopped(false)
{
    poco_check_ptr(factory);
}

void HTTP2ServerConnection::run()
{
    static LoggerPtr log = getLogger("HTTP2ServerConnection");
    LOG_ERROR(log, "got an HTTP/2 connection");
}

}
