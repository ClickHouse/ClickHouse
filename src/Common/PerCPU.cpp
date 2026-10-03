#include <Common/PerCPU.h>

#if defined(OS_LINUX)
#include <sys/sysinfo.h>
#include <fcntl.h>
#include <unistd.h>
#elif defined(OS_DARWIN)
#include <unistd.h>
#endif

#include <algorithm>
#include <charconv>

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
    /// The list is usually a single range, but a sparse one (`0,2,4,...`) on a large machine is
    /// still far below this size.
    char buf[4096];
    int fd = ::open("/sys/devices/system/cpu/possible", O_RDONLY | O_CLOEXEC);
    if (fd < 0)
        return 0;
    ssize_t n = ::read(fd, buf, sizeof(buf));
    [[maybe_unused]] int err = ::close(fd);
    chassert(!err);
    /// A completely filled buffer may hold a truncated list, whose last id would be cut short.
    if (n <= 0 || static_cast<size_t>(n) == sizeof(buf))
        return 0;

    /// Highest id in the list plus one. The storage is indexed by the raw id, so a gap in the
    /// list (theoretically possible: `0-3,8-11`) must count towards the size.
    const char * p = buf;
    const char * const buf_end = buf + n;
    auto parse_id = [&](UInt32 & id)
    {
        auto [ptr, ec] = std::from_chars(p, buf_end, id);
        if (ec != std::errc{} || id >= MAX_POSSIBLE_CPUS)
            return false;
        p = ptr;
        return true;
    };

    UInt32 max_id = 0;
    while (true)
    {
        UInt32 first = 0;
        if (!parse_id(first))
            return 0;
        UInt32 last = first;
        if (p != buf_end && *p == '-')
        {
            ++p;
            if (!parse_id(last) || last < first)
                return 0;
        }
        max_id = std::max(max_id, last);

        if (p == buf_end || *p == '\n')
            return max_id + 1;
        if (*p != ',')
            return 0;
        ++p;
    }
}
#endif

}

UInt32 getNumPossibleCPUs() noexcept
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
        return static_cast<UInt32>(std::min(n, Int64{MAX_POSSIBLE_CPUS}));
    }();
    return cached;
}

UInt32 getNumCPUs() noexcept
{
    return std::min(getNumPossibleCPUs(), MAX_CPUS);
}

}
