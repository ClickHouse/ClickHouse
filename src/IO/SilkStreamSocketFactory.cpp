#include <IO/SilkStreamSocketFactory.h>

#if defined(OS_LINUX)

#include <IO/SilkFiberStreamSocketImpl.h>
#include <Common/Exception.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int SUPPORT_IS_DISABLED;
}
}

namespace Silk
{

std::function<std::unique_ptr<Poco::Net::StreamSocket>(bool secure)> StreamSocketFactory()
{
    return [](bool secure) -> std::unique_ptr<Poco::Net::StreamSocket>
    {
        if (secure)
            return std::make_unique<Poco::Net::StreamSocket>(new SecureFiberStreamSocketImpl);
        return std::make_unique<Poco::Net::StreamSocket>(new FiberStreamSocketImpl);
    };
}

}

#endif
