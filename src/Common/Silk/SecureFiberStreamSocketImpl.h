#pragma once

#include "config.h"

#if USE_SILK && USE_SSL

#include <Poco/Net/Context.h>
#include <Poco/Net/SecureStreamSocketImpl.h>

namespace Silk
{

class SecureFiberStreamSocketImpl final : public Poco::Net::SecureStreamSocketImpl
{
public:
    explicit SecureFiberStreamSocketImpl(Poco::Net::Context::Ptr context);

    /// Server-side socket: adopt an already-accepted plaintext fd and arm the server-side
    /// TLS handshake (performed lazily on the first read or write). Used by a TLS-terminating proxy.
    SecureFiberStreamSocketImpl(int accepted_fd, Poco::Net::Context::Ptr context);

    /// The same over an accepted connection that was served in plaintext so far, for a protocol
    /// that switches to TLS in-band. The fiber socket keeps owning the descriptor.
    SecureFiberStreamSocketImpl(Poco::AutoPtr<Poco::Net::StreamSocketImpl> accepted_socket, Poco::Net::Context::Ptr context);

    bool pollImpl(Poco::Timespan & timeout, int mode) override;
    bool supportsExternalPolling() const override { return false; }
};

}

#endif
