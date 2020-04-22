#if defined(__linux__)

#include <linux/capability.h>

namespace DB
{

/// Check that the current process has Linux capability. Examples: CAP_IPC_LOCK, CAP_NET_ADMIN.
bool hasLinuxCapability(int cap);

}

#endif
