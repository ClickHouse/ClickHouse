#include <Disks/LocalDirectorySyncGuard.h>
#include <Common/ProfileEvents.h>
#include <Common/Exception.h>
#include <Common/ErrnoException.h>
#include <Common/FailPoint.h>
#include <Disks/IDisk.h>
#include <Common/Stopwatch.h>
#include <fcntl.h> // O_RDWR

#include <filesystem>
#include <utility>
#include <vector>

/// OSX does not have O_DIRECTORY
#ifndef O_DIRECTORY
#define O_DIRECTORY O_RDWR
#endif

namespace fs = std::filesystem;

namespace ProfileEvents
{
    extern const Event DirectorySync;
    extern const Event DirectorySyncElapsedMicroseconds;
}

namespace DB
{

namespace FailPoints
{
    extern const char directory_sync_fail[];
}

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
        /// macOS does not declare fdatasync in this build, so use fsync. Unlike
        /// F_FULLFSYNC it does not force a drive-cache flush, matching the
        /// fdatasync semantics used on Linux.
        if (-1 == ::fsync(fd))
            throw Exception(ErrorCodes::CANNOT_FSYNC, "Cannot fsync");
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

CheckedDirectorySync::CheckedDirectorySync(const String & full_path)
    : fd(::open(full_path.c_str(), O_DIRECTORY)), path(full_path)
{
    if (-1 == fd)
        ErrnoException::throwFromPath(
            errno == ENOENT ? ErrorCodes::FILE_DOESNT_EXIST : ErrorCodes::CANNOT_OPEN_FILE,
            full_path, "Cannot open directory {}", full_path);
}

CheckedDirectorySync::~CheckedDirectorySync()
{
    if (fd != -1)
    {
        [[maybe_unused]] int err = ::close(fd);
    }
}

void CheckedDirectorySync::sync()
{
    if (fd == -1)
        return;

    ProfileEvents::increment(ProfileEvents::DirectorySync);
    Stopwatch watch;

    /// Take the descriptor out first: it has to be closed exactly once whether or not the sync
    /// succeeds, and a second call must not retry the sync on a descriptor that is already closed.
    const int dir_fd = std::exchange(fd, -1);

    int sync_errno = 0;
    fiu_do_on(FailPoints::directory_sync_fail, { sync_errno = EIO; });

    if (sync_errno == 0)
    {
#if defined(OS_DARWIN)
        /// macOS does not declare fdatasync in this build, so use fsync. Unlike F_FULLFSYNC it does
        /// not force a drive-cache flush, matching the fdatasync semantics used on Linux.
        if (-1 == ::fsync(dir_fd))
#else
        if (-1 == ::fdatasync(dir_fd))
#endif
            sync_errno = errno;
    }

    if (sync_errno != 0)
    {
        /// The sync failure is what the caller has to act on, so the descriptor is closed here
        /// without reporting a close error that would mask it.
        [[maybe_unused]] int close_err = ::close(dir_fd);
        ErrnoException::throwFromPathWithErrno(
            ErrorCodes::CANNOT_FSYNC, path, sync_errno, "Cannot fsync directory {}", path);
    }

    if (0 != ::close(dir_fd))
        ErrnoException::throwFromPath(ErrorCodes::CANNOT_CLOSE_FILE, path, "Cannot close directory {}", path);

    ProfileEvents::increment(ProfileEvents::DirectorySyncElapsedMicroseconds, watch.elapsedMicroseconds());
}

void createDirectoriesAndSync(const String & dir, bool fsync, std::error_code & ec)
{
    /// Strip a trailing separator so parent_path() walks real components.
    fs::path normalized = dir;
    if (!normalized.has_filename())
        normalized = normalized.parent_path();

    /// Collect the not-yet-existing components before creating them, deepest first.
    std::vector<fs::path> to_create;
    if (fsync)
        for (fs::path p = normalized; !p.empty() && p != p.parent_path() && !fs::exists(p); p = p.parent_path())
            to_create.push_back(p);

    fs::create_directories(normalized, ec);
    if (ec || !fsync)
        return;

    try
    {
        /// Persist each new component in its parent, shallowest first, so a directory only becomes
        /// durably visible after the one containing it. A relative leaf has no directory part of
        /// its own and lives in the current directory.
        for (auto it = to_create.rbegin(); it != to_create.rend(); ++it)
        {
            const auto parent = it->parent_path();
            CheckedDirectorySync parent_sync(parent.empty() ? "." : parent.string());
            parent_sync.sync();
        }
    }
    catch (...)
    {
        /// A directory kept here would be seen as already created by the next call, which would
        /// then never persist its entry. Removal fails on one that is no longer empty, which
        /// belongs to a concurrent writer.
        for (const auto & created : to_create)
        {
            std::error_code remove_ec;
            fs::remove(created, remove_ec);
        }
        throw;
    }
}

void createDirectoriesAndSync(const String & dir, bool fsync)
{
    std::error_code ec;
    createDirectoriesAndSync(dir, fsync, ec);
    if (ec)
        throw fs::filesystem_error("Cannot create directory", dir, ec);
}

}
