#include <Disks/LocalDirectorySyncGuard.h>
#include <Common/ProfileEvents.h>
#include <Common/Exception.h>
#include <Disks/IDisk.h>
#include <Common/Stopwatch.h>
#include <fcntl.h> // O_RDWR

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
    ProfileEvents::increment(ProfileEvents::DirectorySync);

    try
    {
        Stopwatch watch;

#if defined(OS_DARWIN)
        if (fcntl(fd, F_FULLFSYNC, 0))
            throw ErrnoException(ErrorCodes::CANNOT_FSYNC, "Cannot fcntl(F_FULLFSYNC)");
#else
        if (-1 == ::fdatasync(fd))
            throw Exception(ErrorCodes::CANNOT_FSYNC, "Cannot fdatasync");
#endif
        if (-1 == ::close(fd))
            throw Exception(ErrorCodes::CANNOT_CLOSE_FILE, "Cannot close file");

        ProfileEvents::increment(ProfileEvents::DirectorySyncElapsedMicroseconds, watch.elapsedMicroseconds());
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

}
