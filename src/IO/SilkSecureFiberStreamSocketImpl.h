#pragma once

#include "config.h"

#if USE_SILK && USE_SSL

#include <Poco/Net/Context.h>
#include <Poco/Net/SecureStreamSocketImpl.h>

namespace Silk
{

class FiberStreamSocketImpl;

class SecureFiberStreamSocketImpl final : public Poco::Net::SecureStreamSocketImpl
{
public:
    explicit SecureFiberStreamSocketImpl(Poco::Net::Context::Ptr context);

    /// Server-side socket: adopt an already-accepted plaintext fd and arm the server-side
    /// TLS handshake (performed lazily on the first read or write). Used by a TLS-terminating proxy.
    SecureFiberStreamSocketImpl(int accepted_fd, Poco::Net::Context::Ptr context);

    bool getDontWait() const;
    void setDontWait(bool flag);

    bool pollImpl(Poco::Timespan & timeout, int mode) override;
    bool supportsExternalPolling() const override { return false; }

private:
    SecureFiberStreamSocketImpl(FiberStreamSocketImpl * underlying_, Poco::Net::Context::Ptr context);

    FiberStreamSocketImpl * underlying;
};

}

#endif
