#if defined(OS_LINUX)

#include "hasLinuxCapability.h"

#include <syscall.h>
#include <unistd.h>
#include <linux/capability.h>
#include <cstdint>
#include <base/types.h>
#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NETLINK_ERROR;
}

struct Capabilities
{
    UInt64 effective;
    UInt64 permitted;
    UInt64 inheritable;
};

static Capabilities getCapabilities()
{
    /// See man getcap.
    __user_cap_header_struct request{};
    request.version = _LINUX_CAPABILITY_VERSION_3;
    request.pid = getpid();

    Capabilities ret{};
    __user_cap_data_struct response[2] = {};

    /// Avoid dependency on 'libcap'.
    if (0 == syscall(SYS_capget, &request, response))
    {
        ret.effective   = static_cast<UInt64>(response[1].effective) << 32   | response[0].effective;
        ret.permitted   = static_cast<UInt64>(response[1].permitted) << 32   | response[0].permitted;
        ret.inheritable = static_cast<UInt64>(response[1].inheritable) << 32 | response[0].inheritable;
        return ret;
    }

    /// Does not supports V3, fallback to V1.
    /// It's enough to check just single CAP_NET_ADMIN capability we are interested.
    if (errno == EINVAL && 0 == syscall(SYS_capget, &request, response))
    {
        ret.effective = response[0].effective;
        ret.permitted = response[0].permitted;
        ret.inheritable = response[0].inheritable;
        return ret;
    }

    throw ErrnoException(ErrorCodes::NETLINK_ERROR, "Cannot do 'capget' syscall");
}

bool hasLinuxCapability(int cap)
{
    static Capabilities capabilities = getCapabilities();
    return (1 << cap) & capabilities.effective;
}

}

#endif
