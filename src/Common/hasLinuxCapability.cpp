#if defined(OS_LINUX)

#include <Common/hasLinuxCapability.h>

#include <Common/ErrnoException.h>

#include <fmt/format.h>

#include <array>

#include <sys/prctl.h>
#include <sys/syscall.h>
#include <unistd.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int SYSTEM_ERROR;
}

namespace
{

constexpr std::array capability_names = {
    "CAP_CHOWN", "CAP_DAC_OVERRIDE", "CAP_DAC_READ_SEARCH", "CAP_FOWNER", "CAP_FSETID", "CAP_KILL", "CAP_SETGID",
    "CAP_SETUID", "CAP_SETPCAP", "CAP_LINUX_IMMUTABLE", "CAP_NET_BIND_SERVICE", "CAP_NET_BROADCAST", "CAP_NET_ADMIN",
    "CAP_NET_RAW", "CAP_IPC_LOCK", "CAP_IPC_OWNER", "CAP_SYS_MODULE", "CAP_SYS_RAWIO", "CAP_SYS_CHROOT",
    "CAP_SYS_PTRACE", "CAP_SYS_PACCT", "CAP_SYS_ADMIN", "CAP_SYS_BOOT", "CAP_SYS_NICE", "CAP_SYS_RESOURCE",
    "CAP_SYS_TIME", "CAP_SYS_TTY_CONFIG", "CAP_MKNOD", "CAP_LEASE", "CAP_AUDIT_WRITE", "CAP_AUDIT_CONTROL",
    "CAP_SETFCAP", "CAP_MAC_OVERRIDE", "CAP_MAC_ADMIN", "CAP_SYSLOG", "CAP_WAKE_ALARM", "CAP_BLOCK_SUSPEND",
    "CAP_AUDIT_READ", "CAP_PERFMON", "CAP_BPF", "CAP_CHECKPOINT_RESTORE"};

std::string getCapabilityName(unsigned capability)
{
    if (capability < capability_names.size())
        return capability_names[capability];
    return fmt::format("capability {}", capability);
}

bool contains(uint64_t set, unsigned capability)
{
    return (set >> capability) & 1;
}

}

LinuxCapabilities getLinuxCapabilities()
{
    /// Without `libcap`.
    __user_cap_header_struct header{_LINUX_CAPABILITY_VERSION_3, 0};
    __user_cap_data_struct data[2]{};
    if (syscall(SYS_capget, &header, data) != 0)
        throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot get the capabilities of the process");
    return {
        .effective = data[0].effective | (static_cast<uint64_t>(data[1].effective) << 32),
        .permitted = data[0].permitted | (static_cast<uint64_t>(data[1].permitted) << 32),
        .inheritable = data[0].inheritable | (static_cast<uint64_t>(data[1].inheritable) << 32)};
}

bool hasLinuxCapability(int cap)
{
    static const LinuxCapabilities capabilities = getLinuxCapabilities();
    return contains(capabilities.effective, cap);
}

void restrictLinuxCapabilities(uint64_t keep)
{
    const LinuxCapabilities current = getLinuxCapabilities();

    if (contains(current.effective, CAP_SETPCAP))
    {
        for (unsigned capability = 0; capability < 64; ++capability)
        {
            const int in_bounding_set = prctl(PR_CAPBSET_READ, capability, 0, 0, 0);
            if (in_bounding_set < 0)
                break; /// Past the last capability the kernel knows.
            if (in_bounding_set == 0 || contains(keep, capability))
                continue;
            if (prctl(PR_CAPBSET_DROP, capability, 0, 0, 0) != 0)
                throw ErrnoException(
                    ErrorCodes::SYSTEM_ERROR, "Cannot drop {} from the capability bounding set", getCapabilityName(capability));
        }
    }

    __user_cap_header_struct header{_LINUX_CAPABILITY_VERSION_3, 0};
    __user_cap_data_struct data[2]{};
    const LinuxCapabilities restricted{
        .effective = current.effective & keep, .permitted = current.permitted & keep, .inheritable = current.inheritable & keep};
    for (size_t half = 0; half < 2; ++half)
    {
        data[half].effective = static_cast<uint32_t>(restricted.effective >> (32 * half));
        data[half].permitted = static_cast<uint32_t>(restricted.permitted >> (32 * half));
        data[half].inheritable = static_cast<uint32_t>(restricted.inheritable >> (32 * half));
    }
    if (syscall(SYS_capset, &header, data) != 0)
        throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot set the capabilities of the process");
}

std::vector<std::string> getLinuxCapabilityNames(uint64_t set)
{
    std::vector<std::string> names;
    for (unsigned capability = 0; capability < 64; ++capability)
    {
        if (contains(set, capability))
            names.push_back(getCapabilityName(capability));
    }
    return names;
}

}

#endif
