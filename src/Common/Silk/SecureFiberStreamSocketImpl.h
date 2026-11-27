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

    bool pollImpl(Poco::Timespan & timeout, int mode) override;
    bool supportsExternalPolling() const override { return false; }
};

}

#endif
