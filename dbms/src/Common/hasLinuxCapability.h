#if defined(__linux__)

#include <linux/capability.h>

namespace DB
{
bool hasLinuxCapability(decltype(CAP_NET_ADMIN) cap);
}

#endif
