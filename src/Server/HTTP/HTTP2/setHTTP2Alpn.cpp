#include <Server/HTTP/HTTP2/setHTTP2Alpn.h>

#include "config.h"
#if USE_SSL

namespace DB
{

namespace
{

unsigned char ALL_ALPN_PROTOCOLS[] =
{
    0x02, 'h', '2',
    0x08, 'h','t','t','p','/','1','.','1',
    0x08, 'h','t','t','p','/','1','.','0',
};

unsigned char H2_ALPN_PROTOCOLS[] =
{
    0x02, 'h', '2',
};

int alpnSelectCb(
    SSL* /*ssl*/,
    const unsigned char** out,
    unsigned char* outlen,
    const unsigned char* in,
    unsigned int inlen,
    void* /*arg*/)
{
    int res = SSL_select_next_proto(
        const_cast<unsigned char**>(out),
        outlen,
        H2_ALPN_PROTOCOLS,
        sizeof(H2_ALPN_PROTOCOLS),
        in, inlen);
    if (res == OPENSSL_NPN_NEGOTIATED)
        return SSL_TLSEXT_ERR_OK;
    return SSL_TLSEXT_ERR_NOACK;
}

}

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

    SSL_CTX_set_alpn_protos(ssl_ctx, ALL_ALPN_PROTOCOLS, sizeof(ALL_ALPN_PROTOCOLS));
    SSL_CTX_set_alpn_select_cb(ssl_ctx, alpnSelectCb, nullptr);

    return true;
}

}

#endif
