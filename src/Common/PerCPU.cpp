#include <Common/PerCPU.h>

#if defined(OS_LINUX)
#include <sys/sysinfo.h>
#include <fcntl.h>
#include <unistd.h>
#elif defined(OS_DARWIN)
#include <unistd.h>
#endif

#include <algorithm>
#include <cstdlib>

namespace PerCPU
{

namespace
{

#if defined(OS_LINUX)
/// The kernel's `nr_cpu_ids`: `sched_getcpu` never returns an id at or above it. Read from
/// `/sys/devices/system/cpu/possible`, a cpu-list such as `0-127`; returns 0 when the file is
/// unreadable (no sysfs in the chroot) or not a cpu-list.
///
/// This deliberately does not go through the libc. `get_nprocs_conf` (`sysconf(_SC_NPROCESSORS_CONF)`)
/// counts the configured CPUs with glibc but the CPUs in the calling thread's affinity mask with
/// musl, so on a cpuset such as `{32,96}` musl reports 2 while the ids are still 32 and 96 - every
/// per-CPU structure sized by that count would route both CPUs to its fallback shard.
UInt32 readPossibleCPUCount() noexcept
{
    char buf[256];
    int fd = ::open("/sys/devices/system/cpu/possible", O_RDONLY | O_CLOEXEC);
    if (fd < 0)
        return 0;
    ssize_t n = ::read(fd, buf, sizeof(buf) - 1);
    ::close(fd);
    if (n <= 0)
        return 0;
    buf[n] = 0;

    /// Highest id in the list plus one. The storage is indexed by the raw id, so a gap in the
    /// list (theoretically possible: `0-3,8-11`) must count towards the size.
    UInt32 max_id = 0;
    const char * p = buf;
    while (*p)
    {
        char * end;
        long start = std::strtol(p, &end, 10);
        if (end == p || start < 0)
            return 0;
        long last = start;
        if (*end == '-')
        {
            p = end + 1;
            last = std::strtol(p, &end, 10);
            if (end == p || last < start)
                return 0;
        }
        max_id = std::max(max_id, static_cast<UInt32>(last));
        p = end;
        if (*p == ',')
            ++p;
        else if (*p == '\n' || *p == 0)
            break;
        else
            return 0;
    }
    return max_id + 1;
}
#endif

}

UInt32 getNumCPUs() noexcept
{
    static const UInt32 cached = []
    {
#if defined(OS_LINUX)
        Int64 n = readPossibleCPUCount();
        if (n == 0)
            n = get_nprocs_conf();
#elif defined(OS_DARWIN)
        const Int64 n = ::sysconf(_SC_NPROCESSORS_ONLN);
#else
        /// `getCurrentCPU` is not implemented here, so per-CPU routing is impossible; report one
        /// CPU so callers size a single shard instead of creating unreachable ones (e.g. FreeBSD).
        const Int64 n = 1;
#endif
        if (n <= 0)
            return UInt32{1};
        return std::min(static_cast<UInt32>(n), MAX_CPUS);
    }();
    return cached;
}

}
