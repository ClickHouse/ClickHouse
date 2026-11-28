#include <utility>
#include <fcntl.h> // O_RDWR
#include <Disks/IDisk.h>
#include <Disks/LocalDirectorySyncGuard.h>
#include <base/scope_guard.h>
#include <Common/ErrnoException.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/atomicRename.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>

#include <algorithm>
#include <filesystem>
#include <optional>
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
    extern const char local_directory_sync_fail[];
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

    /// Fault injection for tests: the directory sync fails like a disk error would.
    fiu_do_on(FailPoints::local_directory_sync_fail,
    {
        ErrnoException::throwWithErrno(ErrorCodes::CANNOT_FSYNC, EIO, "Injected transient failure while syncing directory");
    });

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

CheckedDirectorySync::CheckedDirectorySync(const String & full_path)
    : path(full_path), fd(::open(path.c_str(), O_DIRECTORY))
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

/// A store path is canonicalized by its caller while the data path keeps whatever form the
/// configuration gave it, so only resolved forms are comparable. Both resolve or neither is used:
/// resolving one alone can miss a containment the raw spellings would have matched.
std::pair<fs::path, fs::path> resolvedForComparison(const fs::path & dir, const fs::path & root)
{
    std::error_code dir_ec;
    fs::path canonical_dir = fs::weakly_canonical(dir, dir_ec);
    std::error_code root_ec;
    fs::path canonical_root = fs::weakly_canonical(root, root_ec);

    if (dir_ec || root_ec)
        return {dir, root};
    return {canonical_dir, canonical_root};
}

/// The components of `dir` that lie below `root`, shallowest first, or nothing at all when `root`
/// does not contain `dir`: with no directory known to predate the store nothing bounds such a
/// walk. An empty list is a `dir` that is `root` itself, whose own entry lies above the boundary.
std::optional<std::vector<fs::path>> componentsBelow(const fs::path & dir, const fs::path & root)
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
    return std::nullopt;
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
        /// A component that was already there is not known to be persisted either: a write that
        /// ran with fsync_metadata disabled, and an operator's mkdir, both leave one behind. An
        /// object is only as durable as every directory holding it, so the whole path is persisted.
        /// Resolved after the creates above, so every component of the path resolves as it now is.
        const auto [resolved_dir, resolved_root] = resolvedForComparison(normalized, root);
        auto below = componentsBelow(resolved_dir, resolved_root);
        if (!below)
        {
            /// Nothing bounds the walk, so persist only what is known to be owed here: the
            /// components this call created, and `dir`'s own entry when it was already there.
            below = created;
            if (below->empty())
                below->push_back(normalized);
        }

        /// Shallowest first, so a directory only becomes durably visible after the one holding it.
        for (const auto & directory : *below)
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

void renameAndSync(const String & from, const String & to, bool replace, bool fsync)
{
    /// Opened before the rename, so a directory that cannot be opened leaves nothing committed.
    std::optional<CheckedDirectorySync> dir_sync;
    if (fsync)
        dir_sync.emplace(fs::path(to).parent_path().string());

    const bool overwrites = dir_sync && replace && fs::exists(to);
    /// An exchange keeps the replaced file at `from` until the sync has succeeded.
    const bool exchanged = overwrites && renameExchangeIfSupported(from, to);
    if (!exchanged)
    {
        if (replace)
            fs::rename(from, to);
        else
            renameNoReplace(from, to);
    }

    if (!dir_sync)
        return;

    try
    {
        dir_sync->sync();
    }
    catch (...)
    {
        try
        {
            if (exchanged)
                renameExchange(from, to);
            else if (!overwrites)
                fs::remove(to);
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__, "Cannot undo the rename of " + to);
        }
        throw;
    }

    if (exchanged)
    {
        std::error_code remove_ec;
        fs::remove(from, remove_ec);
    }
}
}
