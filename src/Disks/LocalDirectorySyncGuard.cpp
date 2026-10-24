#include <utility>
#include <Disks/IDisk.h>
#include <Disks/LocalDirectorySyncGuard.h>
#include <IO/PlatformFileIO.h>
#include <base/scope_guard.h>
#include <Common/ErrnoException.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>

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
    : fd(platformOpenDirectory(full_path))
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

    if (-1 == platformFDataSync(sync_fd))
        ErrnoException::throwWithErrno(ErrorCodes::CANNOT_FSYNC, errno, "Cannot fdatasync directory");
    if (-1 == ::close(std::exchange(sync_fd, -1)))
        ErrnoException::throwWithErrno(ErrorCodes::CANNOT_CLOSE_FILE, errno, "Cannot close directory");

    ProfileEvents::increment(ProfileEvents::DirectorySyncElapsedMicroseconds, watch.elapsedMicroseconds());
}
}
