#pragma once
#if defined(OS_LINUX)

#include <linux/capability.h>

#include <cstdint>
#include <string>
#include <vector>

namespace DB
{

/// Check that the current process has Linux capability. Examples: CAP_IPC_LOCK, CAP_NET_ADMIN.
/// The capabilities are read once, at the first call.
bool hasLinuxCapability(int cap);

/// The capability sets of the calling thread, a bit per capability.
struct LinuxCapabilities
{
    uint64_t effective = 0;
    uint64_t permitted = 0;
    uint64_t inheritable = 0;
};

LinuxCapabilities getLinuxCapabilities();

/// Leaves only the capabilities in `keep` in every set of the calling thread, including the bounding set when that is
/// allowed (it needs `CAP_SETPCAP`). The ambient set loses every capability that leaves the permitted or the
/// inheritable set.
void restrictLinuxCapabilities(uint64_t keep);

/// The names of the capabilities in `set`, such as `CAP_SYS_ADMIN`.
std::vector<std::string> getLinuxCapabilityNames(uint64_t set);

}

#endif
