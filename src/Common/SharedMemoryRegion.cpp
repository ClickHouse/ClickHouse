#include <Common/SharedMemoryRegion.h>

#include <cerrno>
#include <limits>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <Common/Exception.h>
#include <Common/ErrnoException.h>
#include <Common/LockMemoryExceptionInThread.h>
#include <Common/formatReadable.h>
#include <Common/logger_useful.h>
#include <base/defines.h>
#include <base/errnoToString.h>
#include <base/scope_guard.h>

#include <fmt/format.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_OPEN_FILE;
    extern const int CANNOT_FCNTL;
    extern const int CANNOT_ALLOCATE_MEMORY;
    extern const int NOT_IMPLEMENTED;
}

std::string SharedMemoryRegion::pathForChildFd(int child_fd)
{
    return fmt::format("/proc/self/fd/{}", child_fd);
}

#if defined(OS_LINUX)

namespace
{

/// The seals every region carries. `F_SEAL_SHRINK` is the point: the command holds a writable
/// descriptor and must not be able to take pages out from under the server's mapping.
/// `F_SEAL_SEAL` keeps it from adding seals of its own - `F_SEAL_GROW` would break the server's
/// growth, `F_SEAL_WRITE` its writes.
constexpr int REGION_SEALS = F_SEAL_SHRINK | F_SEAL_SEAL;

void closeNoThrow(int fd, const char * operation) noexcept
{
    if (0 != ::close(fd))
    {
        const int close_errno = errno;
        LOG_WARNING(
            getLogger("SharedMemoryRegion"),
            "Cannot close a shared-memory region descriptor during {}: {}",
            operation,
            errnoToString(close_errno));
    }
}

void unmapNoThrow(void * data, size_t size, const char * operation) noexcept
{
    if (0 != ::munmap(data, size))
    {
        const int munmap_errno = errno;
        LOG_WARNING(
            getLogger("SharedMemoryRegion"),
            "Cannot unmap a shared-memory region of {} during {}: {}",
            ReadableSize(size),
            operation,
            errnoToString(munmap_errno));
    }
}

/// `posix_fallocate` both lengthens the file to `size` and commits its pages, and on failure it
/// undoes what it had done, which is what makes it the right single call for creating and growing:
/// there is no separate `ftruncate` whose effect would have to be rolled back - and could not be,
/// on a file sealed against shrinking.
///
/// It is retried on `EINTR`. The server signals itself continuously (the query profiler's timers),
/// the call is not restartable, and reserving a large region is long enough to be caught mid-way.
/// Retrying converges: the pages already reserved stay reserved. Note that `posix_fallocate`
/// reports its error by returning it, not through `errno`.
///
/// The mount behind a `memfd` has no size limit, so the kernel does not refuse an oversized
/// request up front: it keeps committing pages until the machine has none left. The only guard is
/// the caller's, which charges the memory tracker for the size before asking for it.
void reserveBackingStorage(int fd, size_t size, const char * operation)
{
    int fallocate_error = 0;
    do
        fallocate_error = ::posix_fallocate(fd, 0, static_cast<off_t>(size));
    while (fallocate_error == EINTR);

    if (fallocate_error != 0)
        ErrnoException::throwWithErrno(
            ErrorCodes::CANNOT_ALLOCATE_MEMORY,
            fallocate_error,
            "SharedMemoryRegion: Cannot reserve backing storage for {} during {}",
            ReadableSize(size),
            operation);
}

}

void SharedMemoryRegion::checkSupported()
{
    /// Rather than trusting that the kernel offers sealing, ask it: an old kernel or a restricted
    /// container may have `memfd_create` without `MFD_ALLOW_SEALING`, and a region that cannot be
    /// sealed is a region the command can shrink under the server.
    int fd = ::memfd_create("clickhouse_udf_shm_probe", MFD_CLOEXEC | MFD_ALLOW_SEALING);
    if (fd == -1)
    {
        const int saved_errno = errno;
        ErrnoException::throwWithErrno(
            ErrorCodes::NOT_IMPLEMENTED,
            saved_errno,
            "Shared-memory regions for executable UDFs need memfd_create with sealing, which this system does not provide");
    }
    SCOPE_EXIT({ closeNoThrow(fd, "support probe"); });

    /// Every region reserves its pages with `posix_fallocate`, which a seccomp profile may refuse
    /// (`EPERM`, or `EOPNOTSUPP` from a filter that lies about it); ask with one page, so that this
    /// too fails at configuration time rather than on every call.
    reserveBackingStorage(fd, static_cast<size_t>(::sysconf(_SC_PAGESIZE)), "support probe");

    if (0 != ::fcntl(fd, F_ADD_SEALS, REGION_SEALS))
    {
        const int saved_errno = errno;
        ErrnoException::throwWithErrno(
            ErrorCodes::NOT_IMPLEMENTED,
            saved_errno,
            "Shared-memory regions for executable UDFs need file sealing, which this system does not provide");
    }

    /// The command reaches its region by opening `/proc/self/fd/N`, so a system without `procfs`
    /// (a bare `chroot`, a container without it mounted) would load the function and then fail
    /// every call. Ask here instead, the way the command will: open the probe through its own
    /// `/proc/self/fd` entry and check that this leads to the same file.
    int reopened = ::open(pathForChildFd(fd).c_str(), O_RDWR | O_CLOEXEC);
    if (reopened == -1)
    {
        const int saved_errno = errno;
        ErrnoException::throwWithErrno(
            ErrorCodes::NOT_IMPLEMENTED,
            saved_errno,
            "Shared-memory regions for executable UDFs need /proc/self/fd, through which the command opens its region, "
            "and this system does not provide it");
    }
    SCOPE_EXIT({ closeNoThrow(reopened, "support probe"); });

    struct stat original_stat{};
    struct stat reopened_stat{};
    if (0 != ::fstat(fd, &original_stat) || 0 != ::fstat(reopened, &reopened_stat))
    {
        const int saved_errno = errno;
        ErrnoException::throwWithErrno(ErrorCodes::NOT_IMPLEMENTED, saved_errno, "Cannot fstat the shared-memory region probe");
    }
    if (original_stat.st_dev != reopened_stat.st_dev || original_stat.st_ino != reopened_stat.st_ino)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "Shared-memory regions for executable UDFs need /proc/self/fd to lead to the descriptor it names, and on this system it does not");
}

SharedMemoryRegion::SharedMemoryRegion(size_t size)
{
    checkSupported();

    if (size == 0)
        throw Exception(ErrorCodes::CANNOT_ALLOCATE_MEMORY, "SharedMemoryRegion: size must be greater than zero");

    /// `posix_fallocate` takes a signed `off_t`; reject sizes that would overflow it. Defensive: the
    /// executable-UDF loader already bounds configured sizes to `Int64::max`.
    if (size > static_cast<size_t>(std::numeric_limits<off_t>::max()))
        throw Exception(ErrorCodes::CANNOT_ALLOCATE_MEMORY,
            "SharedMemoryRegion: size {} exceeds the maximum {}", size, static_cast<size_t>(std::numeric_limits<off_t>::max()));

    /// Close-on-exec, so that a concurrent `fork` + `exec` on another thread cannot carry this
    /// descriptor into an unrelated child. The one child that should have it gets it explicitly,
    /// through `dup2` - which clears the flag on the copy - before its `exec`.
    int fd = ::memfd_create("clickhouse_udf_shm", MFD_CLOEXEC | MFD_ALLOW_SEALING);
    if (fd == -1)
    {
        const int saved_errno = errno;
        ErrnoException::throwWithErrno(ErrorCodes::CANNOT_OPEN_FILE, saved_errno, "SharedMemoryRegion: Cannot create a memfd");
    }

    try
    {
        reserveBackingStorage(fd, size, "create");

        /// Sealed before it is handed to anyone. The seals are what makes the mapping below safe
        /// to write through for as long as the region lives.
        if (0 != ::fcntl(fd, F_ADD_SEALS, REGION_SEALS))
        {
            const int saved_errno = errno;
            ErrnoException::throwWithErrno(ErrorCodes::CANNOT_FCNTL, saved_errno, "SharedMemoryRegion: Cannot seal the region");
        }

        void * buf = ::mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        if (MAP_FAILED == buf)
        {
            const int saved_errno = errno;
            ErrnoException::throwWithErrno(
                ErrorCodes::CANNOT_ALLOCATE_MEMORY, saved_errno, "SharedMemoryRegion: Cannot mmap {}", ReadableSize(size));
        }

        region_data = static_cast<char *>(buf);
    }
    catch (...)
    {
        /// A constructor that throws leaves no object behind, so its destructor never runs: the
        /// descriptor has to be closed here, or the `tmpfs` pages it holds stay committed - and
        /// charged to nobody - until the server exits.
        closeNoThrow(fd, "region creation cleanup");
        throw;
    }

    region_fd = fd;
    region_size = size;
    backing_size = size;
}

void SharedMemoryRegion::grow(size_t new_size)
{
    if (new_size <= region_size)
        throw Exception(ErrorCodes::CANNOT_ALLOCATE_MEMORY,
            "SharedMemoryRegion: cannot grow from {} to {}: the new size must be larger", ReadableSize(region_size), ReadableSize(new_size));

    if (new_size > static_cast<size_t>(std::numeric_limits<off_t>::max()))
        throw Exception(ErrorCodes::CANNOT_ALLOCATE_MEMORY,
            "SharedMemoryRegion: new size {} exceeds the maximum {}", new_size, static_cast<size_t>(std::numeric_limits<off_t>::max()));

    /// Extends the file and commits the new pages in one call, and leaves the file untouched if it
    /// cannot. Growing is allowed by the seals - only shrinking is refused - so this is the one
    /// thing about the file that changes after creation. A file that had already been grown past
    /// the mapping by an earlier failed remap is left as it is by a request it already satisfies.
    if (new_size > backing_size)
    {
        reserveBackingStorage(region_fd, new_size, "grow");
        backing_size = new_size;
    }

    /// Map the enlarged file into a fresh mapping first; only on success is the old one dropped, so
    /// a failed remap leaves the region fully usable at its previous size. The file is then longer
    /// than the mapping, and the seal means it stays that way: those pages are committed, so
    /// `backing_size` already reports them and the caller keeps them charged, while `region_size`
    /// stays what the transport may actually touch. The next successful growth catches up.
    void * buf = ::mmap(nullptr, new_size, PROT_READ | PROT_WRITE, MAP_SHARED, region_fd, 0);
    if (MAP_FAILED == buf)
    {
        const int saved_errno = errno;
        ErrnoException::throwWithErrno(
            ErrorCodes::CANNOT_ALLOCATE_MEMORY, saved_errno, "SharedMemoryRegion: Cannot mmap {}", ReadableSize(new_size));
    }

    unmapNoThrow(region_data, region_size, "grow");

    region_data = static_cast<char *>(buf);
    region_size = new_size;
}

SharedMemoryRegion::~SharedMemoryRegion()
{
    /// The destructor is implicitly noexcept and logs below, so block memory-limit exceptions: the
    /// region is usually released while its borrow is still charged for it.
    LockMemoryExceptionInThread block_exceptions(VariableContext::Global);

    if (region_data)
        unmapNoThrow(region_data, region_size, "destruction");

    if (region_fd != -1)
        closeNoThrow(region_fd, "destruction");
}

#else

/// Not Linux: no `memfd_create`, no sealing, no `/proc/self/fd`. The loader refuses a function
/// that asks for the transport by calling `checkSupported`, so nothing below runs; it only has
/// to compile.

void SharedMemoryRegion::checkSupported()
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Shared-memory regions for executable UDFs are supported only on Linux");
}

SharedMemoryRegion::SharedMemoryRegion(size_t)
{
    checkSupported();
}

void SharedMemoryRegion::grow(size_t)
{
    checkSupported();
}

SharedMemoryRegion::~SharedMemoryRegion() = default;

#endif

}
