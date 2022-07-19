#include <Disks/LocalDirectorySyncGuard.h>
#include <Common/Exception.h>
#include <Disks/IDisk.h>
#include <fcntl.h> // O_RDWR

/// OSX does not have O_DIRECTORY
#ifndef O_DIRECTORY
#define O_DIRECTORY O_RDWR
#endif

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
        throwFromErrnoWithPath("Cannot open file " + full_path, full_path,
            errno == ENOENT ? ErrorCodes::FILE_DOESNT_EXIST : ErrorCodes::CANNOT_OPEN_FILE);
}

LocalDirectorySyncGuard::~LocalDirectorySyncGuard()
{
    try
    {
#if defined(OS_DARWIN)
        if (fcntl(fd, F_FULLFSYNC, 0))
            throwFromErrno("Cannot fcntl(F_FULLFSYNC)", ErrorCodes::CANNOT_FSYNC);
#endif
        if (-1 == ::fsync(fd))
            throw Exception("Cannot fsync", ErrorCodes::CANNOT_FSYNC);

        if (-1 == ::close(fd))
            throw Exception("Cannot close file", ErrorCodes::CANNOT_CLOSE_FILE);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

}
