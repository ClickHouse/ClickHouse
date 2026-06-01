#pragma once

#if defined(OS_LINUX)

#include <Poco/Net/StreamSocketImpl.h>

namespace Silk
{

class FiberStreamSocketImpl final : public Poco::Net::StreamSocketImpl
{
public:
    FiberStreamSocketImpl() = default;

    explicit FiberStreamSocketImpl(int sockfd);

    void connect(const Poco::Net::SocketAddress & address) override;
    void connect(const Poco::Net::SocketAddress & address, const Poco::Timespan & timeout) override;
    bool pollImpl(Poco::Timespan & timeout, int mode) override;
    int sendBytes(const void * buffer, int length, int flags) override;
    int receiveBytes(void * buffer, int length, int flags) override;
    bool supportsExternalPolling() const override { return false; }
};

}

#endif
