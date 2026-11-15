#include <Common/tryGetFileNameByFileDescriptor.h>

#ifdef OS_LINUX
#    include <unistd.h>
#elif defined(OS_DARWIN)
#    include <fcntl.h>
#endif

#include <fmt/format.h>


namespace DB
{
std::optional<String> tryGetFileNameFromFileDescriptor(int fd)
{
#ifdef OS_LINUX
    std::string proc_path = fmt::format("/proc/self/fd/{}", fd);
    char file_path[PATH_MAX] = {'\0'};
    if (readlink(proc_path.c_str(), file_path, sizeof(file_path) - 1) != -1)
        return file_path;
    return std::nullopt;
#elif defined(OS_DARWIN)
    char file_path[PATH_MAX] = {'\0'};
    if (fcntl(fd, F_GETPATH, file_path) != -1)
        return file_path;
    return std::nullopt;
#else
    (void)fd;
    return std::nullopt;
#endif
}

}
