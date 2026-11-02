#include <utility>
#include <fcntl.h> // O_RDWR
#include <Disks/IDisk.h>
#include <Disks/LocalDirectorySyncGuard.h>
#include <base/scope_guard.h>
#include <Common/ErrnoException.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>

/// OSX does not have O_DIRECTORY
#ifndef O_DIRECTORY
#define O_DIRECTORY O_RDWR
#endif

namespace ProfileEvents
{
    extern const Event DirectorySync;
    extern const Event DirectorySyncElapsedMicroseconds;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_FSYNC;
    extern const int FILE_DOESNT_EXIST;
    extern const int CANNOT_OPEN_FILE;
    extern const int CANNOT_CLOSE_FILE;
}

LocalDirectorySyncGuard::LocalDirectorySyncGuard(const String & full_path)
    : fd(::open(full_path.c_str(), O_DIRECTORY))
{
    if (-1 == fd)
        ErrnoException::throwFromPath(
            errno == ENOENT ? ErrorCodes::FILE_DOESNT_EXIST : ErrorCodes::CANNOT_OPEN_FILE, full_path, "Cannot open file {}", full_path);
}

LocalDirectorySyncGuard::~LocalDirectorySyncGuard()
{
    try
    {
        sync();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void LocalDirectorySyncGuard::sync()
{
    if (fd < 0)
        return;

    int sync_fd = std::exchange(fd, -1);
    SCOPE_EXIT({
        if (sync_fd >= 0)
        {
            [[maybe_unused]] int result = ::close(sync_fd);
        }
    });

    ProfileEvents::increment(ProfileEvents::DirectorySync);
    Stopwatch watch;

#if defined(OS_DARWIN)
    /// macOS does not declare `fdatasync` in this build.
    if (-1 == ::fsync(sync_fd))
        ErrnoException::throwWithErrno(ErrorCodes::CANNOT_FSYNC, errno, "Cannot fsync directory");
#else
    if (-1 == ::fdatasync(sync_fd))
        ErrnoException::throwWithErrno(ErrorCodes::CANNOT_FSYNC, errno, "Cannot fdatasync directory");
#endif
    if (-1 == ::close(std::exchange(sync_fd, -1)))
        ErrnoException::throwWithErrno(ErrorCodes::CANNOT_CLOSE_FILE, errno, "Cannot close directory");

    ProfileEvents::increment(ProfileEvents::DirectorySyncElapsedMicroseconds, watch.elapsedMicroseconds());
}
}
