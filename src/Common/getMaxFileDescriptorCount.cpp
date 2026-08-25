#include <Common/getMaxFileDescriptorCount.h>
#include <sys/resource.h>

std::optional<size_t> getMaxFileDescriptorCount()
{
#if defined(OS_LINUX) || defined(OS_DARWIN)
    /// We want to calculate it only once.
    static auto result = []() -> std::optional<size_t>
    {
        rlimit rlim;
        if (0 != getrlimit(RLIMIT_NOFILE, &rlim))
            return std::nullopt;
        return rlim.rlim_max;
    }();
    return result;
#else
    return std::nullopt;
#endif
}
