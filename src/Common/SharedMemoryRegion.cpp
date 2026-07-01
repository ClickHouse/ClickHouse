#include <Common/SharedMemoryRegion.h>

#include <sys/mman.h>
#include <unistd.h>
#include <fcntl.h>
#include <cstdlib>
#include <cerrno>
#include <limits>
#include <vector>

#include <Common/Exception.h>
#include <Common/ErrnoException.h>
#include <Common/formatReadable.h>
#include <Common/logger_useful.h>
#include <base/defines.h>
#include <base/errnoToString.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_OPEN_FILE;
    extern const int CANNOT_FCNTL;
    extern const int CANNOT_TRUNCATE_FILE;
    extern const int CANNOT_ALLOCATE_MEMORY;
    extern const int NOT_IMPLEMENTED;
}

namespace
{
void reserveBackingStorage([[maybe_unused]] int fd, size_t size, const std::string & operation)
{
#if defined(OS_LINUX)
    int fallocate_error = ::posix_fallocate(fd, 0, static_cast<off_t>(size));
    if (fallocate_error != 0)
    {
        errno = fallocate_error;
        throw ErrnoException(
            ErrorCodes::CANNOT_ALLOCATE_MEMORY,
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
}

void SharedMemoryRegion::checkSupported()
{
#if !defined(OS_LINUX)
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED,
        "Shared memory regions require Linux-specific facilities (`mkostemp`, `posix_fallocate` on a `tmpfs` file) "
        "and are not supported on this platform");
#endif
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

    std::string path_template = directory + "/clickhouse_udf_shm_XXXXXX";
    std::vector<char> path_buffer(path_template.begin(), path_template.end());
    path_buffer.push_back('\0');

    /// Create the temporary file already close-on-exec. Otherwise there is a window between the
    /// file creation and the `fcntl(FD_CLOEXEC)` below in which a concurrent `fork`+`exec` on
    /// another thread would leak this descriptor (holding the mapped UDF data and pinning the
    /// tmpfs storage after `unlink`) into an unrelated child process. `mkostemp` sets the flag
    /// atomically at creation time. The `mkstemp` branch is never taken - `checkSupported` above
    /// throws on non-Linux platforms - it only keeps the file compiling where `mkostemp` is absent.
#if defined(OS_LINUX)
    int fd = ::mkostemp(path_buffer.data(), O_CLOEXEC);
#else
    int fd = ::mkstemp(path_buffer.data());
#endif
    if (fd == -1)
        throw ErrnoException(ErrorCodes::CANNOT_OPEN_FILE, "SharedMemoryRegion: Cannot create file in {}", directory);

    file_path.assign(path_buffer.data());

    /// `mkostemp` already set close-on-exec atomically; this reaffirms it.
    int fd_flags = ::fcntl(fd, F_GETFD);
    if (fd_flags == -1 || ::fcntl(fd, F_SETFD, fd_flags | FD_CLOEXEC) == -1)
    {
        auto saved_errno = errno;
        auto failed_path = file_path;
        ::close(fd);
        ::unlink(file_path.c_str());
        file_path.clear();
        errno = saved_errno;
        throw ErrnoException(ErrorCodes::CANNOT_FCNTL, "SharedMemoryRegion: Cannot set close-on-exec flag for {}", failed_path);
    }

    /// From now on the file exists on disk; make sure it is removed on any failure below.
    auto unlink_on_failure = [&]() noexcept
    {
        ::close(fd);
        ::unlink(file_path.c_str());
        file_path.clear();
    };

    if (0 != ::ftruncate(fd, static_cast<off_t>(size)))
    {
        auto saved_errno = errno;
        unlink_on_failure();
        errno = saved_errno;
        throw ErrnoException(ErrorCodes::CANNOT_TRUNCATE_FILE, "SharedMemoryRegion: Cannot ftruncate to {}", ReadableSize(size));
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
        auto saved_errno = errno;
        unlink_on_failure();
        errno = saved_errno;
        throw ErrnoException(ErrorCodes::CANNOT_ALLOCATE_MEMORY, "SharedMemoryRegion: Cannot mmap {}", ReadableSize(size));
    }

    region_data = static_cast<char *>(buf);

    /// Keep the descriptor open so that `grow` can `ftruncate` and remap without reopening.
    region_fd = fd;
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

    if (0 != ::ftruncate(region_fd, static_cast<off_t>(new_size)))
        throw ErrnoException(ErrorCodes::CANNOT_TRUNCATE_FILE, "SharedMemoryRegion: Cannot ftruncate to {}", ReadableSize(new_size));

    try
    {
        reserveBackingStorage(region_fd, new_size, "grow");
    }
    catch (...)
    {
        if (0 != ::ftruncate(region_fd, static_cast<off_t>(region_size)))
            LOG_WARNING(getLogger("SharedMemoryRegion"),
                "Cannot roll back file size to {} after a failed backing-storage reservation: {}", ReadableSize(region_size), errnoToString());
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
        /// failed query. Shrinking back to region_size is safe — the old mapping still covers
        /// exactly [0, region_size). Preserve the mmap errno for the exception below.
        int mmap_errno = errno;
        if (0 != ::ftruncate(region_fd, static_cast<off_t>(region_size)))
            LOG_WARNING(getLogger("SharedMemoryRegion"),
                "Cannot roll back file size to {} after a failed remap: {}", ReadableSize(region_size), errnoToString());
        errno = mmap_errno;
        throw ErrnoException(ErrorCodes::CANNOT_ALLOCATE_MEMORY, "SharedMemoryRegion: Cannot mmap {}", ReadableSize(new_size));
    }

    if (0 != ::munmap(region_data, region_size))
        LOG_WARNING(getLogger("SharedMemoryRegion"), "Cannot munmap {}: {}", ReadableSize(region_size), errnoToString());

    region_data = static_cast<char *>(buf);
    region_size = new_size;
}

SharedMemoryRegion::~SharedMemoryRegion()
{
    if (region_data)
    {
        if (0 != ::munmap(region_data, region_size))
            LOG_WARNING(getLogger("SharedMemoryRegion"), "Cannot munmap {}: {}", ReadableSize(region_size), errnoToString());
    }

    if (region_fd != -1)
        ::close(region_fd);

    if (!file_path.empty())
    {
        if (0 != ::unlink(file_path.c_str()))
            LOG_WARNING(getLogger("SharedMemoryRegion"), "Cannot unlink {}: {}", file_path, errnoToString());
    }
}

}
