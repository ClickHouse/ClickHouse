#include <Common/SharedMemoryRegion.h>

#include <sys/mman.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <cerrno>
#include <chrono>
#include <filesystem>
#include <limits>
#include <mutex>
#include <unordered_map>

#include <Common/Exception.h>
#include <Common/ErrnoException.h>
#include <Common/LockMemoryExceptionInThread.h>
#include <Common/formatReadable.h>
#include <Common/getRandomASCIIString.h>
#include <Common/logger_useful.h>
#include <base/defines.h>
#include <base/errnoToString.h>
#include <base/scope_guard.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_CREATE_DIRECTORY;
    extern const int CANNOT_OPEN_FILE;
    extern const int CANNOT_CLOSE_FILE;
    extern const int CANNOT_FCNTL;
    extern const int CANNOT_LINK;
    extern const int CANNOT_UNLINK;
    extern const int CANNOT_TRUNCATE_FILE;
    extern const int CANNOT_FSTAT;
    extern const int CANNOT_READ_FROM_FILE_DESCRIPTOR;
    extern const int CANNOT_WRITE_TO_FILE_DESCRIPTOR;
    extern const int UNSUPPORTED_METHOD;
    extern const int CANNOT_STAT;
    extern const int CANNOT_ALLOCATE_MEMORY;
    extern const int NOT_IMPLEMENTED;
}

namespace
{

/// Keep region files in a directory owned exclusively by this mechanism. In particular, the stale
/// file sweep must never apply its filename/lock heuristic to arbitrary files placed directly in a
/// shared directory such as `/dev/shm`.
///
/// The name carries the effective uid, because the parent is normally shared: `/dev/shm` is
/// world-writable, and the directory below it is required to be owned by this user with mode 0700.
/// With one fixed name, whoever created it first owns it for everyone - so a second ClickHouse
/// running as a different OS user could not use the same `shared_memory_path` at all (the layout
/// the documentation describes), and any local user could keep this feature from starting simply by
/// creating that name first. Per-uid names make those two the same case as any other user's
/// directory: not ours, not looked at.
///
/// It is not a defence against a local user who guesses the name and creates it first - nothing
/// placed in a sticky world-writable directory can be - but that fails closed and loudly, at
/// configuration time, rather than silently.
std::string regionDirectoryName()
{
    return fmt::format(".clickhouse-udf-shared-memory-{}", ::geteuid());
}

/// Every region file is named `clickhouse_udf_shm_<random>`, and its owner holds an exclusive
/// `flock` on it for the whole lifetime of the region.
const std::string_view REGION_FILE_PREFIX = "clickhouse_udf_shm_";

/// Region creation can be frequent for non-pooled executable UDFs. Scanning the whole directory
/// for every region would make a burst of N concurrent calls perform O(N^2) directory work. Keep
/// reclamation opportunistic, but run it at most once per directory during this interval.
constexpr std::chrono::minutes STALE_REGION_SCAN_INTERVAL{1};

bool shouldScanForStaleRegions(const std::string & directory)
{
    static std::mutex mutex;
    static std::unordered_map<std::string, std::chrono::steady_clock::time_point> last_scan_by_directory;

    const auto now = std::chrono::steady_clock::now();
    std::lock_guard lock(mutex);
    auto [iterator, inserted] = last_scan_by_directory.try_emplace(directory, now);
    if (inserted)
        return true;

    if (now - iterator->second < STALE_REGION_SCAN_INTERVAL)
        return false;

    iterator->second = now;
    return true;
}

struct LinkedRegionFile
{
    int fd;
    std::string path;
};

void validateDirectory(const std::string & directory)
{
    struct stat directory_stat{};
    if (0 != ::stat(directory.c_str(), &directory_stat))
    {
        const int saved_errno = errno;
        ErrnoException::throwWithErrno(
            ErrorCodes::CANNOT_OPEN_FILE, saved_errno, "SharedMemoryRegion: Cannot inspect directory {}", directory);
    }

    if (!S_ISDIR(directory_stat.st_mode))
        throw Exception(ErrorCodes::CANNOT_OPEN_FILE, "SharedMemoryRegion: {} is not a directory", directory);

    /// Reclamation removes files by name. In a group/world-writable directory without the sticky
    /// bit another user could replace an inspected entry before it is unlinked.
    if ((directory_stat.st_mode & (S_IWGRP | S_IWOTH)) && !(directory_stat.st_mode & S_ISVTX))
        throw Exception(
            ErrorCodes::CANNOT_OPEN_FILE,
            "SharedMemoryRegion: directory {} is group/world-writable but does not have the sticky bit",
            directory);
}

std::string getRegionDirectory(const std::string & parent_directory)
{
    validateDirectory(parent_directory);

    const std::string directory = (std::filesystem::path(parent_directory) / regionDirectoryName()).string();
    bool created = false;
    if (0 == ::mkdir(directory.c_str(), 0700))
    {
        created = true;
    }
    else if (errno != EEXIST)
    {
        const int saved_errno = errno;
        ErrnoException::throwWithErrno(
            ErrorCodes::CANNOT_CREATE_DIRECTORY,
            saved_errno,
            "SharedMemoryRegion: Cannot create private region directory {}",
            directory);
    }

    /// As with region files, a restrictive server umask may clear owner bits from the requested
    /// mode. The new directory is not used until it has the exact private mode.
    if (created && 0 != ::chmod(directory.c_str(), 0700))
    {
        const int saved_errno = errno;
        ErrnoException::throwWithErrno(
            ErrorCodes::CANNOT_CREATE_DIRECTORY,
            saved_errno,
            "SharedMemoryRegion: Cannot set the mode of private region directory {}",
            directory);
    }

    /// Do not follow a pre-existing symlink: the directory is the positive ownership boundary for
    /// stale-file reclamation, so accepting a link would let the sweep escape that boundary.
    struct stat directory_stat{};
    if (0 != ::lstat(directory.c_str(), &directory_stat))
    {
        const int saved_errno = errno;
        ErrnoException::throwWithErrno(
            ErrorCodes::CANNOT_OPEN_FILE,
            saved_errno,
            "SharedMemoryRegion: Cannot inspect private region directory {}",
            directory);
    }

    if (!S_ISDIR(directory_stat.st_mode)
        || directory_stat.st_uid != ::geteuid()
        || (directory_stat.st_mode & 07777) != 0700)
    {
        throw Exception(
            ErrorCodes::CANNOT_OPEN_FILE,
            "SharedMemoryRegion: private region directory {} must be a directory owned by uid {} with mode 0700",
            directory,
            ::geteuid());
    }

    return directory;
}

void unlinkNoThrow(const std::string & path, std::string_view operation) noexcept
{
    if (0 != ::unlink(path.c_str()) && errno != ENOENT)
    {
        const int unlink_errno = errno;
        LOG_WARNING(
            getLogger("SharedMemoryRegion"),
            "Cannot remove shared-memory region file {} during {}: {}",
            path,
            operation,
            errnoToString(unlink_errno));
    }
}

void unmapNoThrow(void * address, size_t size, std::string_view operation) noexcept
{
    if (0 != ::munmap(address, size))
    {
        const int munmap_errno = errno;
        LOG_WARNING(
            getLogger("SharedMemoryRegion"),
            "Cannot unmap shared-memory region of {} during {}: {}",
            ReadableSize(size),
            operation,
            errnoToString(munmap_errno));
    }
}

void closeNoThrow(int fd, std::string_view operation) noexcept
{
    if (0 != ::close(fd))
    {
        const int close_errno = errno;
        LOG_WARNING(
            getLogger("SharedMemoryRegion"),
            "Cannot close shared-memory region file descriptor during {}: {}",
            operation,
            errnoToString(close_errno));
    }
}

/** The three calls below are the ones here that a signal can interrupt, and the server sends itself
  * signals continuously - the query profiler's timers fire on every thread by default. None of them
  * is restartable, so an unretried `EINTR` fails a query for something that has nothing to do with
  * it, and the larger the region the likelier it is: reserving one is exactly the kind of call that
  * is long enough to be caught mid-flight.
  *
  * Retrying is safe for all three. `ftruncate` and `flock` are idempotent, and an interrupted
  * `fallocate` keeps the pages it had already reserved, so every attempt starts closer to done and
  * the loop converges instead of spinning against the signals.
  *
  * `posix_fallocate` is the odd one out in how it reports failure: it returns the error number and
  * leaves `errno` alone.
  */
int ftruncateRetryOnEINTR(int fd, off_t length) noexcept
{
    int res = 0;
    do
        res = ::ftruncate(fd, length);
    while (res != 0 && errno == EINTR);

    return res;
}

int flockRetryOnEINTR(int fd, int operation) noexcept
{
    int res = 0;
    do
        res = ::flock(fd, operation);
    while (res != 0 && errno == EINTR);

    return res;
}

LinkedRegionFile createLinkedRegionFile(const std::string & directory)
{
    validateDirectory(directory);

    /// The file is created unnamed and linked only after it is locked. Therefore a concurrent
    /// stale-region sweep can never observe a live region without its lock.
#if defined(OS_LINUX)
    int fd = ::open(directory.c_str(), O_TMPFILE | O_RDWR | O_CLOEXEC, 0600);
#else
    int fd = -1;
    errno = ENOTSUP;
#endif
    if (fd == -1)
    {
        const int saved_errno = errno;
        ErrnoException::throwWithErrno(
            ErrorCodes::CANNOT_OPEN_FILE,
            saved_errno,
            "SharedMemoryRegion: Cannot create an unnamed file in {} (the filesystem must support `O_TMPFILE`)",
            directory);
    }

    auto close_fd = make_scope_guard([&]
    {
        if (fd != -1)
            closeNoThrow(fd, "region-file creation cleanup");
    });

    /// The mode passed to `open` is masked by the process umask (`<umask>` in the server config), and
    /// this file has to be exactly 0600: the command opens it by path for reading and writing, and
    /// the stale-region sweep below only reclaims files with exactly that mode. A umask that clears
    /// an owner bit (0277, say) would otherwise make every call fail with `EACCES` and leave
    /// leftovers behind forever. The file has no name yet, so it is never visible with another mode.
    if (0 != ::fchmod(fd, 0600))
    {
        const int saved_errno = errno;
        closeNoThrow(fd, "failed mode setup");
        fd = -1;
        ErrnoException::throwWithErrno(
            ErrorCodes::CANNOT_FCNTL, saved_errno, "SharedMemoryRegion: Cannot set the mode of a region file in {}", directory);
    }

    if (0 != flockRetryOnEINTR(fd, LOCK_EX | LOCK_NB))
    {
        const int saved_errno = errno;
        closeNoThrow(fd, "failed lock setup");
        fd = -1;
        ErrnoException::throwWithErrno(
            ErrorCodes::CANNOT_FCNTL, saved_errno, "SharedMemoryRegion: Cannot lock a region file in {}", directory);
    }

    /// Linking through this path also verifies that procfs is mounted and accessible.
    const std::string fd_path = "/proc/self/fd/" + std::to_string(fd);
    static constexpr size_t link_attempts = 16;
    for (size_t attempt = 0;; ++attempt)
    {
        std::string candidate = fmt::format("{}/{}{}", directory, REGION_FILE_PREFIX, getRandomASCIIString(16));

        if (0 == ::linkat(AT_FDCWD, fd_path.c_str(), AT_FDCWD, candidate.c_str(), AT_SYMLINK_FOLLOW))
        {
            LinkedRegionFile result{.fd = fd, .path = std::move(candidate)};
            fd = -1;
            return result;
        }

        if (errno != EEXIST || attempt + 1 == link_attempts)
        {
            const int saved_errno = errno;
            closeNoThrow(fd, "failed region-file linking");
            fd = -1;
            ErrnoException::throwWithErrno(
                ErrorCodes::CANNOT_LINK, saved_errno, "SharedMemoryRegion: Cannot link a region file as {}", candidate);
        }
    }
}

void reserveBackingStorage([[maybe_unused]] int fd, size_t size, const std::string & operation)
{
#if defined(OS_LINUX)
    int fallocate_error = 0;
    do
        fallocate_error = ::posix_fallocate(fd, 0, static_cast<off_t>(size));
    while (fallocate_error == EINTR);

    if (fallocate_error != 0)
    {
        ErrnoException::throwWithErrno(
            ErrorCodes::CANNOT_ALLOCATE_MEMORY,
            fallocate_error,
            "SharedMemoryRegion: Cannot reserve backing storage for {} during {}",
            ReadableSize(size),
            operation);
    }
#else
    /// Not reachable: `checkSupported` rejects non-Linux platforms before a region is ever created.
    /// This branch only keeps the file compiling where `posix_fallocate` is not declared.
    throw Exception(
        ErrorCodes::CANNOT_ALLOCATE_MEMORY,
        "SharedMemoryRegion: Cannot reserve backing storage for {} during {}: `posix_fallocate` is not supported on this platform",
        ReadableSize(size),
        operation);
#endif
}

/// Removes region files left behind by a server that died without running destructors (a `SIGKILL`,
/// an OOM kill, a hard crash). Their `posix_fallocate`d pages stay committed until the file is
/// unlinked, so without this every such exit would pin another region worth of RAM in the `tmpfs`
/// until the machine is rebooted.
///
/// A leftover file is recognized by its lock: the kernel drops an `flock` when the process holding
/// it dies, so a region file that can be locked has no owner left. A live region takes the lock
/// before its file gets a name (see the constructor), which means a region that is visible in the
/// directory is always already locked and can never be mistaken for a leftover. That holds for the
/// regions of this server as well as for those of unrelated ClickHouse instances sharing the same
/// directory, and needs no reliance on pids.
void removeStaleRegions(const std::string & directory)
{
    if (!shouldScanForStaleRegions(directory))
        return;

    auto log = getLogger("SharedMemoryRegion");

    std::error_code error;
    std::filesystem::directory_iterator iterator(directory, error);
    if (error)
    {
        /// Reclamation is best-effort and must not change whether the directory can be used for a
        /// region. In particular, a write-only directory can support `O_TMPFILE` even though it
        /// cannot be listed. The creation below will report its own, more relevant error if needed.
        LOG_DEBUG(log, "Cannot scan {} for leftover shared-memory regions: {}", directory, error.message());
        return;
    }

    const std::filesystem::directory_iterator end;
    for (; iterator != end; iterator.increment(error))
    {
        const auto & entry = *iterator;
        if (!entry.path().filename().string().starts_with(REGION_FILE_PREFIX))
            continue;

        const std::string path = entry.path().string();

        /// Inspect without opening the contents: even an entry with our prefix may be a FIFO or a
        /// device node planted in a shared directory. `O_PATH` has no device-specific side effects.
#if defined(OS_LINUX)
        int path_fd = ::open(path.c_str(), O_PATH | O_CLOEXEC | O_NOFOLLOW);
#else
        int path_fd = -1;
        errno = ENOTSUP;
#endif
        if (path_fd == -1)
        {
            /// Not ours to reclaim: another user's file in a sticky directory, a symlink, or a file
            /// that has just been removed. Leave it alone.
            LOG_DEBUG(log, "Cannot open {} while looking for leftover shared-memory regions: {}", path, errnoToString());
            continue;
        }

        struct stat candidate_stat{};
        if (0 != ::fstat(path_fd, &candidate_stat)
            || !S_ISREG(candidate_stat.st_mode)
            || candidate_stat.st_uid != ::geteuid()
            || (candidate_stat.st_mode & 0777) != 0600
            || candidate_stat.st_nlink != 1)
        {
            closeNoThrow(path_fd, "stale-region candidate rejection");
            continue;
        }

        /// Open the already-inspected inode rather than resolving the directory entry again.
        const std::string fd_path = "/proc/self/fd/" + std::to_string(path_fd);
        int fd = ::open(fd_path.c_str(), O_RDWR | O_CLOEXEC);
        /// Snapshot the reason before closing the inspected descriptor: `close` can overwrite errno.
        const int open_errno = errno;
        closeNoThrow(path_fd, "stale-region candidate inspection");
        if (fd == -1)
        {
            LOG_DEBUG(log, "Cannot reopen {} while looking for leftover shared-memory regions: {}", path, errnoToString(open_errno));
            continue;
        }

        if (0 == flockRetryOnEINTR(fd, LOCK_EX | LOCK_NB))
        {
            /// Confirm that the name still denotes the inspected inode before deleting it. The
            /// configured directory is either non-writable by other users or sticky (validated by
            /// createLinkedRegionFile), so another user cannot replace our entry after this check.
            struct stat path_stat{};
            if (0 != ::lstat(path.c_str(), &path_stat))
            {
                if (errno != ENOENT)
                    LOG_DEBUG(log, "Cannot inspect {} before removing it: {}", path, errnoToString());
            }
            else if (path_stat.st_dev != candidate_stat.st_dev || path_stat.st_ino != candidate_stat.st_ino)
            {
                LOG_DEBUG(log, "Shared-memory region {} changed while it was inspected; leaving it alone", path);
            }
            else if (0 == ::unlink(path.c_str()))
                LOG_INFO(log, "Removed the leftover shared-memory region {} of a process that is gone", path);
            else if (errno != ENOENT)
            {
                const int unlink_errno = errno;
                if (unlink_errno == EACCES || unlink_errno == EPERM)
                    LOG_DEBUG(
                        log,
                        "Cannot remove the shared-memory region {} due to its ownership or directory permissions: {}",
                        path,
                        errnoToString(unlink_errno));
                else
                    LOG_WARNING(log, "Cannot remove the leftover shared-memory region {}: {}", path, errnoToString(unlink_errno));
            }
        }

        closeNoThrow(fd, "stale-region scan");
    }

    if (error)
        LOG_DEBUG(log, "Cannot continue scanning {} for leftover shared-memory regions: {}", directory, error.message());
}

}

void SharedMemoryRegion::checkSupported()
{
#if !defined(OS_LINUX)
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED,
        "Shared memory regions require Linux-specific facilities (`O_TMPFILE`, `posix_fallocate` on a `tmpfs` file) "
        "and are not supported on this platform");
#endif
}

void SharedMemoryRegion::checkSupported(const std::string & directory)
{
    checkSupported();

    const std::string region_directory = getRegionDirectory(directory);

    /// Before the probe below, for the same reason the constructor sweeps before creating a region.
    /// A server that died without running destructors left its files behind, still holding their
    /// pages, and enough of them fill the directory - at which point the probe fails for want of
    /// space and the function never loads. And because creating a region is what would have swept
    /// them away, a failure here means nothing ever will: the leftovers of one crash would keep the
    /// feature unusable until someone cleaned the directory by hand. The configuration path has to
    /// be able to recover on its own, so it reclaims first and asks afterwards.
    removeStaleRegions(region_directory);

    auto [fd, path] = createLinkedRegionFile(region_directory);
    try
    {
        /// A minimal allocation verifies that the filesystem implements `posix_fallocate` without
        /// reserving the configured region size during configuration loading.
        reserveBackingStorage(fd, 1, "configuration validation");
    }
    catch (...)
    {
        unlinkNoThrow(path, "configuration validation cleanup");
        closeNoThrow(fd, "failed configuration validation");
        throw;
    }

    if (0 != ::unlink(path.c_str()))
    {
        const int saved_errno = errno;
        closeNoThrow(fd, "failed configuration-validation cleanup");
        ErrnoException::throwWithErrno(
            ErrorCodes::CANNOT_UNLINK, saved_errno, "SharedMemoryRegion: Cannot remove probe file {}", path);
    }
    if (0 != ::close(fd))
    {
        const int saved_errno = errno;
        ErrnoException::throwWithErrno(
            ErrorCodes::CANNOT_CLOSE_FILE, saved_errno, "SharedMemoryRegion: Cannot close probe file {}", path);
    }
}

SharedMemoryRegion::SharedMemoryRegion(const std::string & directory, size_t size)
    : region_size(size)
{
    /// Fail before anything is created on a platform that cannot back the region. Configuration
    /// paths call this earlier (see `checkSupported`); this is the last line of defence.
    checkSupported();

    if (size == 0)
        throw Exception(ErrorCodes::CANNOT_ALLOCATE_MEMORY, "SharedMemoryRegion: size must be greater than zero");

    /// `ftruncate` takes a signed `off_t`; reject sizes that would overflow it. Defensive: the
    /// executable-UDF loader already bounds configured sizes to `Int64::max`.
    if (size > static_cast<size_t>(std::numeric_limits<off_t>::max()))
        throw Exception(ErrorCodes::CANNOT_ALLOCATE_MEMORY,
            "SharedMemoryRegion: size {} exceeds the maximum {}", size, static_cast<size_t>(std::numeric_limits<off_t>::max()));

    /// Reclamation is confined to a private namespace owned by this mechanism. A matching file in
    /// the configured parent directory is unrelated and must never be considered for deletion.
    const std::string region_directory = getRegionDirectory(directory);

    /// Periodically reclaim the regions of a server that died without running destructors, before
    /// adding one more file to the same directory.
    removeStaleRegions(region_directory);

    /// `O_CLOEXEC` is set atomically during creation, so another thread cannot leak the backing
    /// descriptor into an unrelated child between `open` and `fcntl`.
    auto linked_file = createLinkedRegionFile(region_directory);
    int fd = linked_file.fd;
    file_path = std::move(linked_file.path);

    /// From now on the file exists on disk; make sure it is removed on any failure below - together
    /// with the mapping, once there is one. A constructor that throws leaves no object behind, so
    /// its destructor never runs and a mapping left here is never unmapped; and since the name is
    /// removed at the same time, nothing can free the `tmpfs` pages it holds any more - they stay
    /// committed, and charged to nobody, until the server exits.
    auto unlink_on_failure = [&]() noexcept
    {
        if (region_data)
        {
            unmapNoThrow(region_data, mapped_size, "region creation cleanup");
            region_data = nullptr;
            mapped_size = 0;
        }

        /// `fd` is closed right below; the member must not be left naming a closed descriptor.
        region_fd = -1;

        unlinkNoThrow(file_path, "region creation cleanup");
        closeNoThrow(fd, "region creation cleanup");
        file_path.clear();
    };

    if (0 != ftruncateRetryOnEINTR(fd, static_cast<off_t>(size)))
    {
        const int saved_errno = errno;
        unlink_on_failure();
        ErrnoException::throwWithErrno(
            ErrorCodes::CANNOT_TRUNCATE_FILE, saved_errno, "SharedMemoryRegion: Cannot ftruncate to {}", ReadableSize(size));
    }

    try
    {
        reserveBackingStorage(fd, size, "create");
    }
    catch (...)
    {
        unlink_on_failure();
        throw;
    }

    /// Note: memory accounting is intentionally not done here. The mapping can outlive a single
    /// query (it is reused across `executable_pool` borrows), so the query memory tracker is
    /// charged per borrow by the consumer (`ShellCommandSharedMemorySource`) instead.
    void * buf = ::mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (MAP_FAILED == buf)
    {
        const int saved_errno = errno;
        unlink_on_failure();
        ErrnoException::throwWithErrno(
            ErrorCodes::CANNOT_ALLOCATE_MEMORY, saved_errno, "SharedMemoryRegion: Cannot mmap {}", ReadableSize(size));
    }

    region_data = static_cast<char *>(buf);
    mapped_size = size;

    /// Keep the descriptor open so that `grow` can `ftruncate` and remap without reopening.
    region_fd = fd;

    /// Remember which file this is. The command is given the path and can unlink it or put another
    /// file under that name; from then on the path must not be used, and must not be deleted.
    struct stat file_stat{};
    if (0 != ::fstat(region_fd, &file_stat))
    {
        const int saved_errno = errno;
        unlink_on_failure();
        ErrnoException::throwWithErrno(
            ErrorCodes::CANNOT_FSTAT, saved_errno, "SharedMemoryRegion: Cannot stat the region file {}", file_path);
    }
    file_device = file_stat.st_dev;
    file_inode = file_stat.st_ino;
}

void SharedMemoryRegion::grow(size_t new_size)
{
    if (new_size <= region_size)
        throw Exception(ErrorCodes::CANNOT_ALLOCATE_MEMORY,
            "SharedMemoryRegion: new size {} must be greater than the current size {}",
            ReadableSize(new_size), ReadableSize(region_size));

    if (new_size > static_cast<size_t>(std::numeric_limits<off_t>::max()))
        throw Exception(ErrorCodes::CANNOT_ALLOCATE_MEMORY,
            "SharedMemoryRegion: new size {} exceeds the maximum {}", new_size, static_cast<size_t>(std::numeric_limits<off_t>::max()));

    if (0 != ftruncateRetryOnEINTR(region_fd, static_cast<off_t>(new_size)))
    {
        const int saved_errno = errno;
        ErrnoException::throwWithErrno(
            ErrorCodes::CANNOT_TRUNCATE_FILE, saved_errno, "SharedMemoryRegion: Cannot ftruncate to {}", ReadableSize(new_size));
    }

    try
    {
        reserveBackingStorage(region_fd, new_size, "grow");
    }
    catch (...)
    {
        if (0 != ftruncateRetryOnEINTR(region_fd, static_cast<off_t>(region_size)))
        {
            /// Snapshot before formatting the message: getting the logger can overwrite errno.
            const int rollback_errno = errno;
            LOG_WARNING(getLogger("SharedMemoryRegion"),
                "Cannot roll back file size to {} after a failed backing-storage reservation: {}",
                ReadableSize(region_size), errnoToString(rollback_errno));
        }
        throw;
    }

    /// Map the enlarged file into a fresh mapping first; only on success do we drop the old one,
    /// so a failed remap leaves the region fully usable at its previous size.
    void * buf = ::mmap(nullptr, new_size, PROT_READ | PROT_WRITE, MAP_SHARED, region_fd, 0);
    if (MAP_FAILED == buf)
    {
        /// The file was already enlarged by ftruncate above. Shrink it back so that a failed grow
        /// leaves BOTH the region size and the backing file unchanged. This matters for pooled
        /// processes, which reuse the same SharedMemoryRegion across borrows: otherwise the tmpfs
        /// file would stay larger than region_size, leaking unaccounted memory that outlives the
        /// failed query. Shrinking back to region_size is safe — the old mapping still covers at
        /// least [0, region_size), which is all this region claims. Preserve the mmap errno for the
        /// exception below. If even this rollback fails, the file stays longer than `region_size`;
        /// that is unusable space rather than a hazard, and the next `grow` or `shrink` fixes it.
        const int mmap_errno = errno;
        if (0 != ftruncateRetryOnEINTR(region_fd, static_cast<off_t>(region_size)))
        {
            const int rollback_errno = errno;
            LOG_WARNING(getLogger("SharedMemoryRegion"),
                "Cannot roll back file size to {} after a failed remap: {}", ReadableSize(region_size), errnoToString(rollback_errno));
        }
        ErrnoException::throwWithErrno(
            ErrorCodes::CANNOT_ALLOCATE_MEMORY, mmap_errno, "SharedMemoryRegion: Cannot mmap {}", ReadableSize(new_size));
    }

    if (0 != ::munmap(region_data, mapped_size))
    {
        const int munmap_errno = errno;
        LOG_WARNING(getLogger("SharedMemoryRegion"), "Cannot munmap {}: {}", ReadableSize(mapped_size), errnoToString(munmap_errno));
    }

    region_data = static_cast<char *>(buf);
    region_size = new_size;
    mapped_size = new_size;
}

void SharedMemoryRegion::readBackingFile(char * destination, size_t offset, size_t size) const
{
    size_t bytes_read = 0;
    while (bytes_read < size)
    {
        const ssize_t res = ::pread(
            region_fd, destination + bytes_read, size - bytes_read, static_cast<off_t>(offset + bytes_read));

        if (res < 0)
        {
            if (errno == EINTR)
                continue;

            const int saved_errno = errno;
            ErrnoException::throwWithErrno(
                ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR,
                saved_errno,
                "SharedMemoryRegion: Cannot read {} bytes at offset {} of {}",
                size,
                offset,
                file_path);
        }

        /// End of file inside the range the command told the server to read: it shortened the file
        /// after answering. Through the mapping this would have been a `SIGBUS`.
        if (res == 0)
            throw Exception(
                ErrorCodes::UNSUPPORTED_METHOD,
                "SharedMemoryRegion: the region file {} ends after {} of the {} bytes reported at offset {}; "
                "only the server may resize the region",
                file_path,
                bytes_read,
                size,
                offset);

        bytes_read += static_cast<size_t>(res);
    }
}

void SharedMemoryRegion::writeBackingFile(const char * source, size_t offset, size_t size)
{
    /// Against what the region claims, not against what the file happens to be right now: the file
    /// is the command's to change, and a length read from it would be a check against the very
    /// thing that cannot be trusted. `pwrite` past the end of a shortened file is not a fault - it
    /// extends the file - so this bound is about the region's own accounting, not about safety.
    if (offset > region_size || size > region_size - offset)
        throw Exception(
            ErrorCodes::UNSUPPORTED_METHOD,
            "SharedMemoryRegion: refusing to write {} bytes at offset {} of a region of {} bytes",
            size,
            offset,
            region_size);

    size_t bytes_written = 0;
    while (bytes_written < size)
    {
        const ssize_t res = ::pwrite(
            region_fd, source + bytes_written, size - bytes_written, static_cast<off_t>(offset + bytes_written));

        if (res < 0)
        {
            if (errno == EINTR)
                continue;

            const int saved_errno = errno;
            ErrnoException::throwWithErrno(
                ErrorCodes::CANNOT_WRITE_TO_FILE_DESCRIPTOR,
                saved_errno,
                "SharedMemoryRegion: Cannot write {} bytes at offset {} of {}",
                size,
                offset,
                file_path);
        }

        /// `pwrite` returning zero for a non-zero request is not something a regular file does;
        /// treat it as an error rather than spinning.
        if (res == 0)
            throw Exception(
                ErrorCodes::CANNOT_WRITE_TO_FILE_DESCRIPTOR,
                "SharedMemoryRegion: wrote nothing of the {} bytes remaining at offset {} of {}",
                size - bytes_written,
                offset + bytes_written,
                file_path);

        bytes_written += static_cast<size_t>(res);
    }
}

SharedMemoryRegion::BackingFileState SharedMemoryRegion::backingFileState() const
{
    struct stat file_stat{};
    if (0 != ::fstat(region_fd, &file_stat))
    {
        const int saved_errno = errno;
        ErrnoException::throwWithErrno(
            ErrorCodes::CANNOT_FSTAT, saved_errno, "SharedMemoryRegion: Cannot stat the region file {}", file_path);
    }

    BackingFileState state{.size = static_cast<size_t>(file_stat.st_size), .path_is_ours = false};

    /// The descriptor keeps the file alive even after it loses its name, so the size above says
    /// nothing about the path. Look the path up separately: it has to still lead to this very file.
    struct stat path_stat{};
    if (0 == ::lstat(file_path.c_str(), &path_stat))
        state.path_is_ours = path_stat.st_dev == file_device && path_stat.st_ino == file_inode;
    else if (errno != ENOENT)
    {
        const int saved_errno = errno;
        ErrnoException::throwWithErrno(
            ErrorCodes::CANNOT_STAT, saved_errno, "SharedMemoryRegion: Cannot inspect the region path {}", file_path);
    }

    return state;
}

void SharedMemoryRegion::shrink(size_t new_size) noexcept
{
    chassert(new_size > 0 && new_size < region_size);
    if (new_size == 0 || new_size >= region_size)
        return;

    /// This runs while the caller (a borrow that is being torn down) is still charged for the whole
    /// region, so an allocation in the logging below could hit the memory limit. That exception
    /// would escape a `noexcept` function and terminate the server, and it would be pointless
    /// anyway: this method only gives memory back.
    LockMemoryExceptionInThread block_exceptions(VariableContext::Global);

    auto log = getLogger("SharedMemoryRegion");

    /// Give the backing storage back first. The consumer accounts a pooled region by what `size`
    /// reports, so the file must never be left larger than that: those pages would stay pinned for
    /// the lifetime of the worker with nothing accounting for them. Truncating under the existing
    /// mapping is safe - it only takes away the tail, which the region no longer claims - and it
    /// keeps a failure atomic: the region is then completely unchanged.
    if (0 != ftruncateRetryOnEINTR(region_fd, static_cast<off_t>(new_size)))
    {
        LOG_WARNING(log, "Cannot ftruncate a shared-memory region down to {}: {}", ReadableSize(new_size), errnoToString());
        return;
    }

    region_size = new_size;

    /// Replace the now-oversized mapping. A failure here costs only address space: the region keeps
    /// the old mapping, whose first `new_size` bytes are exactly the ones the file still backs, and
    /// `mapped_size` remembers how much of it has to be unmapped later.
    void * buf = ::mmap(nullptr, new_size, PROT_READ | PROT_WRITE, MAP_SHARED, region_fd, 0);
    if (MAP_FAILED == buf)
    {
        LOG_WARNING(log, "Cannot mmap {} while shrinking a shared-memory region: {}", ReadableSize(new_size), errnoToString());
        return;
    }

    if (0 != ::munmap(region_data, mapped_size))
        LOG_WARNING(log, "Cannot munmap {}: {}", ReadableSize(mapped_size), errnoToString());

    region_data = static_cast<char *>(buf);
    mapped_size = new_size;
}

size_t SharedMemoryRegion::reconcileBackingFileSize() noexcept
{
    /// Runs while the borrow handing this region over is still charged for it, so an allocation in
    /// the logging below could hit the memory limit - and that exception would escape a `noexcept`
    /// function. Same reason as in `shrink`.
    LockMemoryExceptionInThread block_exceptions(VariableContext::Global);

    auto log = getLogger("SharedMemoryRegion");

    struct stat file_stat{};
    if (0 != ::fstat(region_fd, &file_stat))
    {
        LOG_WARNING(log, "Cannot inspect the region file {} while handing it over: {}", file_path, errnoToString());
        return region_size;
    }

    const size_t file_size = static_cast<size_t>(file_stat.st_size);

    /// A file that got shorter is the other direction entirely: it is a hazard rather than an
    /// accounting error, it is what `backingFileState` reports and what the consumer's integrity
    /// check acts on, and extending it again here would hide it. Nothing to do, and nothing extra
    /// to charge - the region is charged for what it reserved.
    if (file_size <= region_size)
        return region_size;

    LOG_WARNING(
        log,
        "The region file {} holds {} rather than the {} this region claims, so something resized it. "
        "Putting it back, so the extra pages are released instead of staying charged to nobody.",
        file_path,
        ReadableSize(file_size),
        ReadableSize(region_size));

    if (0 == ftruncateRetryOnEINTR(region_fd, static_cast<off_t>(region_size)))
        return region_size;

    LOG_WARNING(
        log, "Cannot put the region file {} back to {}: {}", file_path, ReadableSize(region_size), errnoToString());

    /// The pages are there whether or not they can be given back, so they are charged for. Reporting
    /// the smaller number would leave them counted by nobody, which is the one answer that is wrong
    /// in a direction nothing else corrects.
    return file_size;
}

SharedMemoryRegion::~SharedMemoryRegion()
{
    /// The destructor is implicitly noexcept and logs below, so block memory-limit exceptions for
    /// the same reason as in `shrink`: the region is usually released while its borrow is still
    /// charged for it.
    LockMemoryExceptionInThread block_exceptions(VariableContext::Global);

    /// Hoisted out of the log calls below: getting the logger can overwrite the errno they report.
    auto log = getLogger("SharedMemoryRegion");

    if (region_data)
    {
        if (0 != ::munmap(region_data, mapped_size))
            LOG_WARNING(log, "Cannot munmap {}: {}", ReadableSize(mapped_size), errnoToString());
    }

    if (!file_path.empty())
    {
        /// Delete the name only while it still leads to this file: the command could have removed
        /// it and created something else under it, and that something else is not ours to remove.
        struct stat path_stat{};
        if (0 != ::lstat(file_path.c_str(), &path_stat))
        {
            if (errno != ENOENT)
                LOG_WARNING(log, "Cannot inspect {} before removing it: {}", file_path, errnoToString());
        }
        else if (path_stat.st_dev != file_device || path_stat.st_ino != file_inode)
        {
            LOG_WARNING(log, "Not removing {}: it no longer names the region file this object owns", file_path);
        }
        else if (0 != ::unlink(file_path.c_str()))
            LOG_WARNING(log, "Cannot unlink {}: {}", file_path, errnoToString());
    }

    if (region_fd != -1)
        closeNoThrow(region_fd, "region destruction");
}

}
