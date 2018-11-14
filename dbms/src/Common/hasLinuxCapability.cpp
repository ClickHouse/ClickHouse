#include "hasLinuxCapability.h"

#if defined(__linux__)

#include <syscall.h>
#include <unistd.h>
#include <linux/capability.h>
#include <linux/netlink.h>
#include <Common/Exception.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NETLINK_ERROR;
}


namespace
{
    bool hasLinuxCapabilityImpl(decltype(CAP_NET_ADMIN) cap)
    {
        /// See man getcap.
        __user_cap_header_struct request{};
        request.version = _LINUX_CAPABILITY_VERSION_1; /// It's enough to check just single CAP_NET_ADMIN capability we are interested.
        request.pid = getpid();

        __user_cap_data_struct response{};

        /// Avoid dependency on 'libcap'.
        if (0 != syscall(SYS_capget, &request, &response))
            throwFromErrno("Cannot do 'capget' syscall", ErrorCodes::NETLINK_ERROR);

        if (!((1 << cap) & response.effective))
            return false;

        return true;
    }
}

bool hasLinuxCapability(decltype(CAP_NET_ADMIN) cap)
{
    static bool res = hasLinuxCapabilityImpl(cap);
    return res;
}
}

#endif
