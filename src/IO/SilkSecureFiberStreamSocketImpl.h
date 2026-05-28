#pragma once

#if defined(OS_LINUX)

#include <Poco/Net/Context.h>
#include <Poco/Net/SecureStreamSocketImpl.h>

namespace Silk
{

class SecureFiberStreamSocketImpl final : public Poco::Net::SecureStreamSocketImpl
{
public:
    explicit SecureFiberStreamSocketImpl(Poco::Net::Context::Ptr context);

    bool pollImpl(Poco::Timespan & timeout, int mode) override;
};

}

#endif
