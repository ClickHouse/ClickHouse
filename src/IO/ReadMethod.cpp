#include <IO/ReadMethod.h>
#include <IO/preadNoWait.h>

#include <base/defines.h>

#include <fcntl.h>
#include <unistd.h>


namespace DB
{

bool willUseDirectIO([[maybe_unused]] size_t estimated_size, [[maybe_unused]] size_t direct_io_threshold)
{
#if defined(OS_LINUX) || defined(OS_FREEBSD)
    return direct_io_threshold && estimated_size >= direct_io_threshold;
#else
    /// `createReadBufferFromFileBase` does not even compile the O_DIRECT branch here.
    return false;
#endif
}

bool canOpenWithDirectIO([[maybe_unused]] const std::string & path)
{
#if defined(OS_LINUX) || defined(OS_FREEBSD)
    int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC | O_DIRECT);
    if (fd == -1)
        return false;
    [[maybe_unused]] int res = ::close(fd);
    chassert(res == 0);
    return true;
#else
    /// `createReadBufferFromFileBase` does not even compile the O_DIRECT branch here.
    return false;
#endif
}

LocalFSReadMethod resolveLocalFSReadMethod(LocalFSReadMethod requested, bool pread_no_wait_supported, bool direct_io)
{
    if (requested == LocalFSReadMethod::pread_threadpool && !pread_no_wait_supported && !direct_io)
        return LocalFSReadMethod::pread;

    return requested;
}

LocalFSReadMethod resolveLocalFSReadMethod(LocalFSReadMethod requested, bool direct_io)
{
    /// Other methods do not depend on `preadNoWait`, and reads with O_DIRECT never look at
    /// the page cache, so neither must reach the probe: it is a raw system call that
    /// a kill-on-deny `seccomp` profile terminates the process for.
    if (requested != LocalFSReadMethod::pread_threadpool || direct_io)
        return requested;

    return resolveLocalFSReadMethod(requested, getPreadNoWaitSupport().supported, direct_io);
}

}
