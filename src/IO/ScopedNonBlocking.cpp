#include <IO/ScopedNonBlocking.h>

#include "config.h"

#include <Common/Exception.h>
#include <Poco/Net/SocketImpl.h>

#if USE_SILK && USE_SSL
#include <IO/SilkSecureFiberStreamSocketImpl.h>
#endif

namespace DB
{

ScopedNonBlocking::ScopedNonBlocking(Poco::Net::SocketImpl & socket_impl_)
    : socket_impl(socket_impl_)
{
#if USE_SILK && USE_SSL
    /// The Silk TLS BIO is always non-blocking so that an OpenSSL operation cannot
    /// suspend and migrate between the operation and `SSL_get_error`.
    if (dynamic_cast<Silk::SecureFiberStreamSocketImpl *>(&socket_impl))
        return;
#endif
    was_blocking = socket_impl.getBlocking();
    if (was_blocking)
        socket_impl.setBlocking(false);
}

ScopedNonBlocking::~ScopedNonBlocking()
{
    if (!was_blocking)
        return;

    try
    {
        socket_impl.setBlocking(true);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

}
