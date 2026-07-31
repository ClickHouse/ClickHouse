#include <Disks/LocalDirectorySyncGuard.h>
#include <Common/ProfileEvents.h>
#include <Common/Exception.h>
#include <Common/ErrnoException.h>
#include <Disks/IDisk.h>
#include <Common/Stopwatch.h>
#include <IO/PlatformFileIO.h>

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
    ProfileEvents::increment(ProfileEvents::DirectorySync);

    try
    {
        Stopwatch watch;

        if (-1 == platformFDataSync(fd))
            throw Exception(ErrorCodes::CANNOT_FSYNC, "Cannot fdatasync");
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
