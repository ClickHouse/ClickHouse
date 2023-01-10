#if defined(OS_LINUX)

#include "hasLinuxCapability.h"

#include <syscall.h>
#include <unistd.h>
#include <linux/capability.h>
#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NETLINK_ERROR;
}

static __user_cap_data_struct getCapabilities()
{
    /// See man getcap.
    __user_cap_header_struct request{};
    request.version = _LINUX_CAPABILITY_VERSION_1; /// It's enough to check just single CAP_NET_ADMIN capability we are interested.
    request.pid = getpid();

    __user_cap_data_struct response{};

    /// Avoid dependency on 'libcap'.
    if (0 != syscall(SYS_capget, &request, &response))
        throwFromErrno("Cannot do 'capget' syscall", ErrorCodes::NETLINK_ERROR);

    return response;
}

bool hasLinuxCapability(int cap)
{
    static __user_cap_data_struct capabilities = getCapabilities();
    return (1 << cap) & capabilities.effective;
}

}

#endif
