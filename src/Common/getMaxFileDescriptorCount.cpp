#include <Common/getMaxFileDescriptorCount.h>
#include <sys/resource.h>

std::optional<size_t> getMaxFileDescriptorCount()
{
    static auto result = []() -> std::optional<size_t>
    {
    #if defined(OS_LINUX) || defined(OS_DARWIN)
        rlimit rlim;
        if (getrlimit(RLIMIT_NOFILE, &rlim))
            return std::nullopt;
        return rlim.rlim_max;
    #else
        return std::nullopt;
    #endif
    }();

    return result;
}
