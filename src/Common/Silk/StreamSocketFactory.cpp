#include <Common/Silk/StreamSocketFactory.h>

#if USE_SILK

#include <Common/Silk/FiberStreamSocketImpl.h>
#include <Common/Exception.h>

#if USE_SSL
#include <Common/Silk/SecureFiberStreamSocketImpl.h>
#include <Poco/Net/SSLManager.h>
#include <Poco/Net/SecureStreamSocket.h>
#endif

namespace DB
{
namespace ErrorCodes
{
    extern const int SUPPORT_IS_DISABLED;
}
}

namespace Silk
{

std::function<std::unique_ptr<Poco::Net::StreamSocket>(bool secure)> streamSocketFactory()
{
    return [](bool secure) -> std::unique_ptr<Poco::Net::StreamSocket>
    {
        if (secure)
        {
#if USE_SSL
            auto context = Poco::Net::SSLManager::instance().defaultClientContext();
            Poco::Net::StreamSocket socket(new SecureFiberStreamSocketImpl(context));
            return std::make_unique<Poco::Net::SecureStreamSocket>(socket);
#else
            throw DB::Exception(DB::ErrorCodes::SUPPORT_IS_DISABLED,
                "tcp_secure protocol is disabled because poco library was built without NetSSL support.");
#endif
        }
        return std::make_unique<Poco::Net::StreamSocket>(new FiberStreamSocketImpl);
    };
}

}

#endif
