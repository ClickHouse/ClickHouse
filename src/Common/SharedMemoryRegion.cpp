#include <Common/SharedMemoryRegion.h>

#include <algorithm>
#include <cerrno>
#include <cstdlib>
#include <string>
#include <limits>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/syscall.h>
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

/// The sealing constants of the kernel ABI (`<linux/fcntl.h>`, `<linux/memfd.h>`), for a libc whose
/// headers predate them - the sysroot of one cross-compilation target does. The kernel has had
/// them since 3.17, with these values, on every architecture; a libc header that declares them
/// agrees, and one that does not gets them from here.
#if !defined(F_ADD_SEALS)
#    define F_ADD_SEALS 1033
#endif
#if !defined(F_SEAL_SEAL)
#    define F_SEAL_SEAL 0x0001
#endif
#if !defined(F_SEAL_SHRINK)
#    define F_SEAL_SHRINK 0x0002
#endif
#if !defined(MFD_CLOEXEC)
#    define MFD_CLOEXEC 0x0001U
#endif
#if !defined(MFD_ALLOW_SEALING)
#    define MFD_ALLOW_SEALING 0x0002U
#endif

namespace
{

/// The seals every region carries. `F_SEAL_SHRINK` is the point: the command holds a writable
/// descriptor and must not be able to make the file shorter than the server's mapping - that is
/// the one thing that would turn an access into a `SIGBUS`. `F_SEAL_SEAL` keeps it from adding
/// seals of its own - `F_SEAL_GROW` would break the server's growth, `F_SEAL_WRITE` its writes.
/// Nothing here stops the command from extending the file or from punching holes in it: the
/// former is measured at every hand-over (`refreshBackingSize`), the latter is the command's own
/// loss (the class comment states the contract); neither can crash the server.
constexpr int REGION_SEALS = F_SEAL_SHRINK | F_SEAL_SEAL;

/// The raw system call rather than the glibc wrapper: the wrapper is `memfd_create@GLIBC_2.27`,
/// and the binary must not depend on glibc symbols newer than 2.4 (the compatibility check runs
/// it on very old distributions). The kernel has had the call since 3.17; on an older one it
/// fails with `ENOSYS`, which `checkSupported` reports like any other absence.
int memfdCreate(const char * name, unsigned int flags)
{
    return static_cast<int>(::syscall(SYS_memfd_create, name, flags));
}

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
/// It is retried on `EINTR`, as a matter of form more than of need: `shmem_fallocate` checks only
/// for a *fatal* pending signal (`fatal_signal_pending`), so the query profiler's timers do not
/// interrupt it, and a call that was interrupted unwinds the pages it had allocated (`undo`), so
/// a retry starts over rather than closer to the end. Note that `posix_fallocate` reports its
/// error by returning it, not through `errno`.
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
    int fd = memfdCreate("clickhouse_udf_shm_probe", MFD_CLOEXEC | MFD_ALLOW_SEALING);
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
    /// too fails at configuration time rather than on every call. Whatever the reason, it is
    /// reported as the transport being unavailable here - this is a probe, and a refusal of one
    /// page is not the machine running out of memory.
    int fallocate_error = 0;
    do
        fallocate_error = ::posix_fallocate(fd, 0, static_cast<off_t>(::sysconf(_SC_PAGESIZE)));
    while (fallocate_error == EINTR);
    if (fallocate_error != 0)
        ErrnoException::throwWithErrno(
            ErrorCodes::NOT_IMPLEMENTED,
            fallocate_error,
            "Shared-memory regions for executable UDFs need posix_fallocate on a memfd, which this system refuses");

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
    /// No `checkSupported` here: the loader ran the probe once, when the function was loaded, and
    /// running it again on every region would report a transient failure of the real creation
    /// below - out of descriptors, out of memory - as the transport being unavailable. What
    /// fails below reports as what it is.
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
    int fd = memfdCreate("clickhouse_udf_shm", MFD_CLOEXEC | MFD_ALLOW_SEALING);
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
    reserved_size = size;
    committed_size = roundUpToPages(size);
    footprint_size = roundUpToPages(size);
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
    /// thing about the file that changes after creation. Always, even when the file is already
    /// long enough: it may have been made so by an earlier growth that then failed to map (whose
    /// pages are committed, and this is a no-op) or by the command with a plain `ftruncate`, which
    /// leaves a sparse tail whose pages are not - and the server is about to map and write them.
    /// `posix_fallocate` over committed pages costs a walk, not a copy.
    reserveBackingStorage(region_fd, new_size, "grow");
    backing_size = std::max(backing_size, new_size);
    reserved_size = std::max(reserved_size, new_size);
    committed_size = std::max(committed_size, roundUpToPages(new_size));
    footprint_size = std::max(footprint_size, roundUpToPages(new_size));

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

size_t SharedMemoryRegion::refreshBackingSize()
{
    struct stat st{};
    if (0 != ::fstat(region_fd, &st))
    {
        const int saved_errno = errno;
        ErrnoException::throwWithErrno(ErrorCodes::CANNOT_FCNTL, saved_errno, "SharedMemoryRegion: Cannot fstat the region");
    }

    /// Never below what is known: the seals make a shorter file impossible, so a smaller figure
    /// here would be a bug, not a fact.
    backing_size = std::max(backing_size, static_cast<size_t>(st.st_size));
    return backing_size;
}

size_t SharedMemoryRegion::refreshFootprint()
{
    struct stat st{};
    if (0 != ::fstat(region_fd, &st))
    {
        const int saved_errno = errno;
        ErrnoException::throwWithErrno(ErrorCodes::CANNOT_FCNTL, saved_errno, "SharedMemoryRegion: Cannot fstat the region");
    }

    backing_size = std::max(backing_size, static_cast<size_t>(st.st_size));
    /// `st_blocks` is in 512-byte units whatever the page size, and for a `memfd` it is exactly
    /// the pages the file holds - inside its length or past it. Whole pages on both sides of the
    /// comparison: a length is rounded up to the page it ends in, which the file holds either way.
    /// Never less than what was seen before: pages come and go (a hole the command punched), but
    /// a charge that went down with them would have to be taken again when they come back, on the
    /// hot path, uncounted; the footprint is a high-water mark, like the length.
    committed_size = static_cast<size_t>(st.st_blocks) * 512;
    footprint_size = std::max({footprint_size, roundUpToPages(backing_size), committed_size});
    return footprint_size;
}

namespace
{

/// Reads a small sysfs file into `out`; false if it cannot be read.
bool readSysfsLine(const char * path, std::string & out)
{
    int fd = ::open(path, O_RDONLY | O_CLOEXEC);
    if (fd == -1)
        return false;
    SCOPE_EXIT({ closeNoThrow(fd, "sysfs probe"); });

    char buffer[256];
    ssize_t bytes = 0;
    do
        bytes = ::read(fd, buffer, sizeof(buffer) - 1);
    while (bytes == -1 && errno == EINTR);
    if (bytes <= 0)
        return false;

    out.assign(buffer, static_cast<size_t>(bytes));
    return true;
}

/// The unit a `memfd` is backed in: the page, unless the kernel backs `shmem` with transparent
/// huge pages regardless of file size (`shmem_enabled` is `always` or `force`), in which case a
/// file of any length holds at least one huge page and `st_blocks` says so - a 64 KiB region
/// reports 2 MiB. Footprints and caps have to be compared in that unit, or a region would be over
/// its own cap from the moment it is created (`within_size` and `advise` only use huge pages
/// where they fit, and are the page as far as this is concerned). Read once.
size_t backingUnit()
{
    const size_t page_size = static_cast<size_t>(::sysconf(_SC_PAGESIZE));

    std::string mode;
    if (!readSysfsLine("/sys/kernel/mm/transparent_hugepage/shmem_enabled", mode))
        return page_size;
    if (!mode.contains("[always]") && !mode.contains("[force]"))
        return page_size;

    std::string huge_page_size;
    if (!readSysfsLine("/sys/kernel/mm/transparent_hugepage/hpage_pmd_size", huge_page_size))
        return page_size;
    const size_t huge = std::strtoull(huge_page_size.c_str(), nullptr, 10);
    return huge > page_size ? huge : page_size;
}

}

size_t SharedMemoryRegion::roundUpToPages(size_t size)
{
    static const size_t unit = backingUnit();
    return (size + unit - 1) / unit * unit;
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

size_t SharedMemoryRegion::refreshBackingSize()
{
    checkSupported();
    return 0;
}

size_t SharedMemoryRegion::refreshFootprint()
{
    checkSupported();
    return 0;
}

size_t SharedMemoryRegion::roundUpToPages(size_t size)
{
    return size;
}

SharedMemoryRegion::~SharedMemoryRegion() = default;

#endif

}
