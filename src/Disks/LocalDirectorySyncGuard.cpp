#include <Disks/LocalDirectorySyncGuard.h>
#include <Common/ProfileEvents.h>
#include <Common/Exception.h>
#include <Common/ErrnoException.h>
#include <Common/FailPoint.h>
#include <Disks/IDisk.h>
#include <Common/Stopwatch.h>
#include <fcntl.h> // O_RDWR

#include <algorithm>
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

namespace
{

/// A relative leaf has no directory part of its own and lives in the current directory.
void syncParentOf(const fs::path & path)
{
    const auto parent = path.parent_path();
    CheckedDirectorySync parent_sync(parent.empty() ? "." : parent.string());
    parent_sync.sync();
}

/// Deepest first, since a directory still holding another cannot be removed.
void removeDirectories(const std::vector<fs::path> & dirs)
{
    for (auto it = dirs.rbegin(); it != dirs.rend(); ++it)
    {
        std::error_code remove_ec;
        fs::remove(*it, remove_ec);
    }
}

/// The components of `dir` that lie below `root`, shallowest first. Empty when `root` does not
/// contain `dir`: with no directory known to predate the store, nothing bounds such a walk.
std::vector<fs::path> componentsBelow(const fs::path & dir, const fs::path & root)
{
    std::vector<fs::path> result;
    for (fs::path p = dir; p != p.parent_path(); p = p.parent_path())
    {
        if (p == root)
        {
            std::reverse(result.begin(), result.end());
            return result;
        }
        result.push_back(p);
    }
    return {};
}

}

void createDirectoriesAndSync(const String & dir, bool fsync, std::error_code & ec, const String & existing_root)
{
    /// Strip a trailing separator so parent_path() walks real components.
    fs::path normalized = dir;
    if (!normalized.has_filename())
        normalized = normalized.parent_path();

    fs::path root = existing_root;
    if (!root.empty() && !root.has_filename())
        root = root.parent_path();

    if (!fsync)
    {
        fs::create_directories(normalized, ec);
        return;
    }

    /// Collect the not-yet-existing components before creating them, deepest first.
    std::vector<fs::path> missing;
    for (fs::path p = normalized; !p.empty() && p != p.parent_path() && !fs::exists(p); p = p.parent_path())
        missing.push_back(p);

    /// Created one at a time, shallowest first: fs::create_directories does not report which
    /// components it created, and only the directories this call created may be removed again
    /// below. A single create returning false without an error is one that appeared in between,
    /// so it belongs to whoever created it.
    std::vector<fs::path> created;
    for (auto it = missing.rbegin(); it != missing.rend(); ++it)
    {
        std::error_code create_ec;
        const bool is_new = fs::create_directory(*it, create_ec);
        if (create_ec)
        {
            /// A half-made path left here would be taken for a finished one by the next call,
            /// which would then never persist the entries of the components already present.
            removeDirectories(created);
            ec = create_ec;
            return;
        }
        if (is_new)
            created.push_back(*it);
    }
    ec.clear();

    try
    {
        /// A component that was already there may come from a write that ran with fsync_metadata
        /// disabled, or from an operator's mkdir, so its entry is not known to be persisted
        /// either. An object committed in the store is only as durable as every directory
        /// holding it, so the whole path below `root` is persisted, not only what was created.
        auto to_persist = componentsBelow(normalized, root);
        if (to_persist.empty())
        {
            /// Nothing bounds the walk, so persist only what is known to be owed here: the
            /// components this call created, and `dir`'s own entry when it was already there.
            to_persist = created;
            if (to_persist.empty())
                to_persist.push_back(normalized);
        }

        /// Shallowest first, so a directory only becomes durably visible after the one holding it.
        for (const auto & directory : to_persist)
            syncParentOf(directory);
    }
    catch (...)
    {
        removeDirectories(created);
        throw;
    }
}

void createDirectoriesAndSync(const String & dir, bool fsync, const String & existing_root)
{
    std::error_code ec;
    createDirectoriesAndSync(dir, fsync, ec, existing_root);
    if (ec)
        throw fs::filesystem_error("Cannot create directory", dir, ec);
}

}
