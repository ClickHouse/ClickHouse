#include <Frontend.h>

#if USE_SILK

namespace DB::Proxy
{

/// MySQL is server-speaks-first and negotiates TLS in-band, so the proxy cannot read the user name or
/// the database before it has already answered the greeting. Routing therefore uses the peer address
/// or the listener's default pool, and the whole connection (including any in-band TLS upgrade) is
/// forwarded transparently. Routing by hostname requires SNI, which is only available after the
/// in-band TLS handshake and is not inspected here.
void handleMySQL(FiberSocket & client, const FrontendContext & ctx)
{
    RouteAttributes attributes;
    attributes.protocol = ListenerProtocol::MySQL;
    handleImmediatePassthrough(client, ctx, attributes);
}

}

#endif
