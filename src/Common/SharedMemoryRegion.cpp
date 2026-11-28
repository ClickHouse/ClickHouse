#include <Common/SharedMemoryRegion.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <charconv>
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <string>
#include <string_view>
#include <limits>
#include <optional>
#include <mutex>
#include <vector>

#include <fcntl.h>
#include <pthread.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <unistd.h>

#if defined(OS_LINUX)
#include <sys/sysmacros.h>
#endif

#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <Common/Exception.h>
#include <Common/ErrnoException.h>
#include <Common/LockMemoryExceptionInThread.h>
#include <Common/formatReadable.h>
#include <Common/logger_useful.h>
#include <base/defines.h>
#include <base/errnoToString.h>
#include <base/getPageSize.h>
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
    extern const int CANNOT_PARSE_TEXT;
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

std::mutex region_registry_mutex;
SharedMemoryRegion * first_region = nullptr;

/// The seals every region carries. `F_SEAL_SHRINK` is the point: the command holds a writable
/// descriptor and must not be able to make the file shorter than the server's mapping - that is
/// the one thing that would turn an access into a `SIGBUS`. `F_SEAL_SEAL` keeps it from adding
/// seals of its own - `F_SEAL_GROW` would break the server's growth, `F_SEAL_WRITE` its writes.
/// Nothing here stops the command from extending the file or from punching holes in it: the
/// former is measured at every hand-over (`refreshFootprint`), the latter is the command's own
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
        /// The message allocates, and these run where an exception cannot leave: in `SCOPE_EXIT` and
        /// in destructors, usually while the query is at its memory limit.
        LockMemoryExceptionInThread block_exceptions(VariableContext::Global);
        LOG_WARNING(
            getLogger("SharedMemoryRegion"),
            "Cannot close a shared-memory region descriptor during {}: {}",
            operation,
            errnoToString(close_errno));
    }
}

/// The largest folio a `memfd` was seen to be backed in (`checkSupported`): a lower bound for the unit
/// `roundUpToPages` rounds to, whatever `/sys` says. Only ever raised.
std::atomic<size_t> observed_backing_unit{0};

void observeBackingUnit(size_t bytes)
{
    size_t current = observed_backing_unit.load(std::memory_order_relaxed);
    while (bytes > current && !observed_backing_unit.compare_exchange_weak(current, bytes, std::memory_order_relaxed))
    {
    }
}

/// `posix_fallocate` of the file from `offset` up to `size`. Returns the error, which `posix_fallocate`
/// returns rather than sets in `errno`; 0 on success.
///
/// Before Linux 6.12 `shmem_fallocate` gives up on *any* pending signal (`signal_pending`), not only
/// a fatal one, and undoes every page it had committed - so a retry starts from the beginning, and a
/// region that takes longer to commit than the period of a timer signal aimed at this thread (the
/// query profiler's) would never be committed at all: every attempt is interrupted. So once an
/// attempt has been interrupted, the retries run with every signal blocked in this thread; such a
/// signal stays pending and is delivered once the call returns, and only a fatal one, which cannot be
/// blocked, still interrupts it - and then the retry is moot. Not blocked up front: the commit can
/// take seconds for a large region, and signals the server aims at this thread (the profiler's, the
/// stack-trace collection of `system.stack_trace`) would wait that long - on a recent kernel, and on
/// an old one that is not interrupted, for nothing.
int fallocateUpTo(int fd, size_t offset, size_t size)
{
    const off_t length = static_cast<off_t>(size - offset);
    int fallocate_error = ::posix_fallocate(fd, static_cast<off_t>(offset), length);
    if (fallocate_error != EINTR)
        return fallocate_error;

    sigset_t all_signals;
    sigset_t previous_signals;
    sigfillset(&all_signals);
    pthread_sigmask(SIG_BLOCK, &all_signals, &previous_signals);
    SCOPE_EXIT({ pthread_sigmask(SIG_SETMASK, &previous_signals, nullptr); });

    do
        fallocate_error = ::posix_fallocate(fd, static_cast<off_t>(offset), length);
    while (fallocate_error == EINTR);
    return fallocate_error;
}

/// Frees every page of the file, inside its length and past it. 0 on success, -1 with `errno` set.
int punchWholeFile(int fd)
{
    const off_t page_size = static_cast<off_t>(getPageSize());
    const off_t whole_file = std::numeric_limits<off_t>::max() / page_size * page_size;
    return ::fallocate(fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, 0, whole_file);
}

/// Frees every page of a region's file - inside its length and past it - for whoever still holds
/// it. A region is dropped when the server is done with it, and that is when its charge is dropped
/// too; the pages have to go at the same moment, or the charge would be gone while they stay. They
/// would stay as long as anything else holds the file: a command that ignored the termination
/// signal its destructor gets, or a descendant that inherited the descriptor. Punching a hole is
/// allowed under the seals (they forbid shrinking, not freeing), the file keeps its length, and a
/// process that still maps it reads zeros; whatever it writes after that allocates pages of its
/// own, as any of its memory would be.
void releasePagesNoThrow(int fd) noexcept
{
    if (0 != punchWholeFile(fd))
    {
        const int punch_errno = errno;
        /// As in `closeNoThrow`.
        LockMemoryExceptionInThread block_exceptions(VariableContext::Global);
        LOG_WARNING(
            getLogger("SharedMemoryRegion"),
            "Cannot free the pages of a shared-memory region on its destruction; they stay until the last "
            "descriptor of it is closed: {}",
            errnoToString(punch_errno));
    }
}

void unmapNoThrow(void * data, size_t size, const char * operation) noexcept
{
    if (0 != ::munmap(data, size))
    {
        const int munmap_errno = errno;
        /// As in `closeNoThrow`.
        LockMemoryExceptionInThread block_exceptions(VariableContext::Global);
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
/// A call that was interrupted unwinds the pages it had allocated (`undo`), so a retry starts over
/// rather than closer to the end - which is why `fallocateUpTo` retries with signals blocked. Note that
/// `posix_fallocate` reports its error by returning it, not through `errno`.
///
/// The mount behind a `memfd` has no size limit, so the kernel does not refuse an oversized
/// request up front: it keeps committing pages until the machine has none left. The only guard is
/// the caller's, which charges the memory tracker for the size before asking for it.
void reserveBackingStorage(int fd, size_t offset, size_t size, const char * operation)
{
    const int fallocate_error = fallocateUpTo(fd, offset, size);
    if (fallocate_error != 0)
        ErrnoException::throwWithErrno(
            ErrorCodes::CANNOT_ALLOCATE_MEMORY,
            fallocate_error,
            "SharedMemoryRegion: Cannot reserve backing storage for {} during {}",
            ReadableSize(size),
            operation);
}

/// The probes of `checkSupported`, in the order it runs them. Each asks the kernel for one thing a
/// region relies on, on a probe `memfd`, and throws `NOT_IMPLEMENTED` if it is not there - so that
/// a system that lacks it is reported once, when the function is loaded, rather than on every call.

/// Rather than trusting that the kernel offers sealing, ask it: an old kernel or a restricted
/// container may have `memfd_create` without `MFD_ALLOW_SEALING`, and a region that cannot be
/// sealed is a region the command can shrink under the server. Returns the probe descriptor, which
/// the caller closes.
int createSealableProbeMemfd()
{
    int fd = memfdCreate("clickhouse_udf_shm_probe", MFD_CLOEXEC | MFD_ALLOW_SEALING);
    if (fd == -1)
    {
        const int saved_errno = errno;
        ErrnoException::throwWithErrno(
            ErrorCodes::NOT_IMPLEMENTED,
            saved_errno,
            "Shared-memory regions for executable UDFs need memfd_create with sealing, which this system does not provide");
    }
    return fd;
}

/// Every region reserves its pages with `posix_fallocate`, which a seccomp profile may refuse
/// (`EPERM`, or `EOPNOTSUPP` from a filter that lies about it); ask with one page, so that this
/// too fails at configuration time rather than on every call. Whatever the reason, it is
/// reported as the transport being unavailable here - this is a probe, and a refusal of one
/// page is not the machine running out of memory.
void probeFallocate(int fd)
{
    const int fallocate_error = fallocateUpTo(fd, 0, static_cast<size_t>(getPageSize()));
    if (fallocate_error != 0)
        ErrnoException::throwWithErrno(
            ErrorCodes::NOT_IMPLEMENTED,
            fallocate_error,
            "Shared-memory regions for executable UDFs need posix_fallocate on a memfd, which this system refuses");
}

/// Footprints and caps are compared in the unit the kernel backs a `memfd` in (`roundUpToPages`),
/// which is read from `/sys/kernel/mm/transparent_hugepage`. Without `/sys/kernel/mm` at all (a
/// chroot, a container without `/sys`) it cannot be told, and a unit taken to be the page while
/// the kernel backs the file in larger folios would charge a region for less than it holds - so
/// the transport is refused rather than run on a guess. (`/sys/kernel/mm` without
/// `transparent_hugepage` is a kernel without transparent huge pages, where the page is right.)
void requireSysKernelMm()
{
    if (0 != ::access("/sys/kernel/mm", R_OK | X_OK))
    {
        const int saved_errno = errno;
        ErrnoException::throwWithErrno(
            ErrorCodes::NOT_IMPLEMENTED,
            saved_errno,
            "Shared-memory regions for executable UDFs need /sys/kernel/mm to tell the unit a memfd is backed in, "
            "and this system does not provide it");
    }
}

/// And what the kernel actually committed for the page `probeFallocate` asked for is a lower bound
/// for the unit, whatever `/sys` says: never lowered by this, only raised.
void observeProbeBackingUnit(int fd)
{
    struct stat probe_stat{};
    if (0 != ::fstat(fd, &probe_stat))
    {
        const int saved_errno = errno;
        ErrnoException::throwWithErrno(ErrorCodes::NOT_IMPLEMENTED, saved_errno, "Cannot fstat the shared-memory region probe");
    }
    observeBackingUnit(static_cast<size_t>(probe_stat.st_blocks) * 512);
    /// The `/sys` part is read now, once, so that a knob that cannot be read or parsed fails the load
    /// of the function rather than a query.
    SharedMemoryRegion::roundUpToPages(0);
}

/// Adds the seals every region carries (`REGION_SEALS`), so that the probes after this one run
/// under them, as they would on a region.
void probeSealing(int fd)
{
    if (0 != ::fcntl(fd, F_ADD_SEALS, REGION_SEALS))
    {
        const int saved_errno = errno;
        ErrnoException::throwWithErrno(
            ErrorCodes::NOT_IMPLEMENTED,
            saved_errno,
            "Shared-memory regions for executable UDFs need file sealing, which this system does not provide");
    }
}

/// A region's pages are freed with `FALLOC_FL_PUNCH_HOLE` when the server drops it - at the
/// moment its charge is dropped (`releasePagesNoThrow`) - and when it is cleared for another
/// borrower (`releasePagesUpToLength`). A system that allows `posix_fallocate` but not the hole
/// punch would load the function and then keep pages nobody is charged for, so ask here, under
/// the seals the region has, and check by the pages the file holds afterwards that the pages
/// are really gone: a filter can answer success without doing anything.
void probePunchHole(int fd)
{
    if (0 != punchWholeFile(fd))
    {
        const int saved_errno = errno;
        ErrnoException::throwWithErrno(
            ErrorCodes::NOT_IMPLEMENTED,
            saved_errno,
            "Shared-memory regions for executable UDFs need fallocate with FALLOC_FL_PUNCH_HOLE on a memfd, which this system refuses");
    }

    struct stat punched_stat{};
    if (0 != ::fstat(fd, &punched_stat))
    {
        const int saved_errno = errno;
        ErrnoException::throwWithErrno(ErrorCodes::NOT_IMPLEMENTED, saved_errno, "Cannot fstat the shared-memory region probe");
    }
    if (punched_stat.st_blocks != 0)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "Shared-memory regions for executable UDFs need fallocate with FALLOC_FL_PUNCH_HOLE to free the pages of a memfd, "
            "and on this system it does not: {} bytes are still held after it",
            static_cast<size_t>(punched_stat.st_blocks) * 512);
}

/// The command reaches its region by opening `/proc/self/fd/N`, so a system without `procfs`
/// (a bare `chroot`, a container without it mounted) would load the function and then fail
/// every call. Ask here instead, the way the command will: open the probe through its own
/// `/proc/self/fd` entry and check that this leads to the same file.
void probeReopenThroughProcSelfFd(int fd)
{
    int reopened = ::open(SharedMemoryRegion::pathForChildFd(fd).c_str(), O_RDWR | O_CLOEXEC);
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

}

void SharedMemoryRegion::checkSupported(bool require_process_rss)
{
    /// The OS memory source must be able to exclude region mappings before adding their charge.
    /// Reject an unsupported configuration here, rather than disabling background corrections
    /// with an unreadable `smaps` as soon as the first pooled region is created.
    if (require_process_rss)
    {
        char buffer[4096];
        ReadBufferFromFile smaps("/proc/self/smaps", sizeof(buffer), -1, buffer);
        if (smaps.eof())
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Shared-memory RSS accounting requires a nonempty /proc/self/smaps");
    }

    const int fd = createSealableProbeMemfd();
    SCOPE_EXIT({ closeNoThrow(fd, "support probe"); });

    probeFallocate(fd);
    requireSysKernelMm();
    observeProbeBackingUnit(fd);
    probeSealing(fd);
    probePunchHole(fd);
    probeReopenThroughProcSelfFd(fd);
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

    /// Before the descriptor exists: the first call reads `/sys` and may throw, and nothing may
    /// throw outside the `try` below once there is a descriptor to leak.
    const size_t footprint = roundUpToPages(size);

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
        struct stat st{};
        if (0 != ::fstat(fd, &st))
            ErrnoException::throwWithErrno(ErrorCodes::CANNOT_FCNTL, errno, "SharedMemoryRegion: Cannot identify the region");
        region_device = st.st_dev;
        region_inode = st.st_ino;

        reserveBackingStorage(fd, 0, size, "create");

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
    committed_size = footprint;
    footprint_size = footprint;

    /// An intrusive registry cannot fail to allocate after the mapping has been created.
    std::lock_guard lock(region_registry_mutex);
    next_region = first_region;
    if (next_region)
        next_region->previous_region = this;
    first_region = this;
}

uint64_t SharedMemoryRegion::residentMemoryWithoutRegions(const MemoryStatisticsOS & statistics)
{
    /// Keep the descriptors alive until the read completes, so an inode cannot be reused for an
    /// unrelated mapping. `grow` needs no registry update: both mappings refer to the same file.
    std::lock_guard lock(region_registry_mutex);
    if (!first_region)
        return statistics.get().resident;

    std::vector<std::pair<uint64_t, uint64_t>> files;
    for (auto * region = first_region; region; region = region->next_region)
        files.emplace_back(region->region_device, region->region_inode);
    std::sort(files.begin(), files.end());

    /// Avoid allocating the default 1 MiB read buffer on every tick: its allocator retention
    /// (or sanitizer quarantine) would itself inflate the memory being measured.
    char buffer[16384];
    ReadBufferFromFile in("/proc/self/smaps", sizeof(buffer), -1, buffer);
    std::string line;
    uint64_t resident = 0;
    bool exclude = false;
    bool seen_mapping = false;
    bool seen_rss = false;
    const auto parse_number = [](std::string_view token, auto & value, int base = 10)
    {
        if (token.empty())
            return false;
        const auto [end, error] = std::from_chars(token.data(), token.data() + token.size(), value, base);
        return error == std::errc{} && end == token.data() + token.size();
    };
    while (!in.eof())
    {
        line.clear();
        readStringUntilNewlineInto(line, in);
        if (!in.eof())
            in.ignore();

        std::string_view remaining = line;
        const auto next_token = [&remaining]() -> std::string_view
        {
            const auto start = remaining.find_first_not_of(" \t");
            if (start == std::string_view::npos)
            {
                remaining = {};
                return {};
            }
            remaining.remove_prefix(start);
            const auto token = remaining.substr(0, remaining.find_first_of(" \t"));
            remaining.remove_prefix(token.size());
            return token;
        };

        if (!line.empty() && ((line[0] >= '0' && line[0] <= '9') || (line[0] >= 'a' && line[0] <= 'f')))
        {
            next_token(); /// Address range.
            next_token(); /// Permissions.
            next_token(); /// File offset.
            const auto device = next_token();
            const auto separator = device.find(':');
            uint32_t device_major = 0;
            uint32_t device_minor = 0;
            uint64_t inode = 0;
            if ((seen_mapping && !seen_rss)
                || separator == std::string_view::npos
                || !parse_number(device.substr(0, separator), device_major, 16)
                || !parse_number(device.substr(separator + 1), device_minor, 16)
                || !parse_number(next_token(), inode))
                throw Exception(ErrorCodes::CANNOT_PARSE_TEXT, "Cannot parse a mapping in /proc/self/smaps");
            const std::pair<uint64_t, uint64_t> file{makedev(device_major, device_minor), inode};
            exclude = std::binary_search(files.begin(), files.end(), file);
            seen_mapping = true;
            seen_rss = false;
        }
        else if (line.starts_with("Rss:"))
        {
            next_token(); /// `Rss:` label.
            uint64_t rss_kb = 0;
            if (!seen_mapping || seen_rss || !parse_number(next_token(), rss_kb))
                throw Exception(ErrorCodes::CANNOT_PARSE_TEXT, "Cannot parse RSS in /proc/self/smaps");
            if (!exclude)
                resident += rss_kb * 1024;
            seen_rss = true;
        }
    }
    if (!seen_mapping || !seen_rss)
        throw Exception(ErrorCodes::CANNOT_PARSE_TEXT, "Missing RSS in /proc/self/smaps");
    return resident;
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
    /// thing about the file that changes after creation. From what the server itself last
    /// committed (`reservedSize`), even when the file is already long enough: the command may have
    /// made it so with a plain `ftruncate`, which leaves a sparse tail whose pages are not committed
    /// - and the server is about to map and write them. Not from the start of the file: that would
    /// walk every page the region already has on every growth, and commit again the pages the
    /// command freed inside it, taking the footprint past what `fillCostUpTo` bounded the growth by.
    /// A page freed there is allocated on its next access, as it would be without a growth.
    if (new_size > reserved_size)
        reserveBackingStorage(region_fd, reserved_size, new_size, "grow");
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

size_t SharedMemoryRegion::releasePagesUpToLength()
{
    refreshFootprint();
    const size_t length = backing_size;
    const off_t range = static_cast<off_t>(roundUpToPages(length));
    if (0 != ::fallocate(region_fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, 0, range))
    {
        const int saved_errno = errno;
        ErrnoException::throwWithErrno(
            ErrorCodes::CANNOT_ALLOCATE_MEMORY, saved_errno, "SharedMemoryRegion: Cannot free the pages of the region up to {}",
            ReadableSize(length));
    }

    /// Read back rather than assumed to be zero: the pages past the range are the command's, and
    /// nothing else says how many there are.
    refreshFootprint();
    return committed_size;
}

void SharedMemoryRegion::recommitUpToLength()
{
    reserveBackingStorage(region_fd, 0, backing_size, "recommit");
    reserved_size = std::max(reserved_size, backing_size);
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

/// The first line of a sysfs file, or nothing if the kernel has no such file: a knob it does not
/// have. A file that is there and cannot be read is an error.
std::optional<std::string> readSysfsLine(const std::string & path)
{
    if (!std::filesystem::exists(path))
        return std::nullopt;
    ReadBufferFromFile in(path);
    std::string line;
    readStringUntilNewlineInto(line, in);
    return line;
}

bool isAlwaysOrForce(const std::string & mode)
{
    return mode.contains("[always]") || mode.contains("[force]");
}

/// The unit a `memfd` is backed in: the page, unless `shmem` is backed with large folios regardless
/// of the file's size, in which case a file of any length holds at least one such folio and
/// `st_blocks` says so - a 64 KiB region reports 2 MiB. That is transparent huge pages with
/// `shmem_enabled` `always` or `force`, and from Linux 6.11 on any size whose own
/// `hugepages-<size>kB/shmem_enabled` is `always` (or `inherit`, with the former). Footprints and caps
/// have to be compared in that unit, or a region would be over its own cap from the moment it is
/// created (`within_size` and `advise` use large folios only where they fit, and are the page as far
/// as this is concerned). The largest of them: a unit that is too large rounds a cap up by at most
/// one such folio, while one that is too small discards every worker whose region the kernel backs
/// with larger folios. Read once; `roundUpToPages` raises it further to what `checkSupported` saw the
/// kernel commit for a page.
size_t backingUnit()
{
    size_t unit = static_cast<size_t>(getPageSize());

    static const std::string thp_dir = "/sys/kernel/mm/transparent_hugepage";

    const auto top_mode = readSysfsLine(thp_dir + "/shmem_enabled");
    const bool top_always = top_mode && isAlwaysOrForce(*top_mode);

    /// Before the per-size knobs, the PMD size is the one large folio there is.
    if (top_always)
    {
        if (const auto pmd_size = readSysfsLine(thp_dir + "/hpage_pmd_size"))
            unit = std::max(unit, parse<size_t>(*pmd_size));
    }

    std::error_code error;
    for (auto it = std::filesystem::directory_iterator(thp_dir, error); !error && it != std::filesystem::directory_iterator();
         it.increment(error))
    {
        const std::string name = it->path().filename().string();
        if (!name.starts_with("hugepages-") || !name.ends_with("kB"))
            continue;

        const auto mode = readSysfsLine(it->path().string() + "/shmem_enabled");
        if (!mode)
            continue;
        if (!mode->contains("[always]") && !(mode->contains("[inherit]") && top_always))
            continue;

        const std::string_view size_kb(name.begin() + std::strlen("hugepages-"), name.end() - std::strlen("kB"));
        unit = std::max(unit, parse<size_t>(size_kb.data(), size_kb.size()) * 1024);
    }

    return unit;
}

}

size_t SharedMemoryRegion::roundUpToPages(size_t size)
{
    static const size_t sysfs_unit = backingUnit();
    const size_t unit = std::max(sysfs_unit, observed_backing_unit.load(std::memory_order_relaxed));
    return (size + unit - 1) / unit * unit;
}

SharedMemoryRegion::~SharedMemoryRegion()
{
    /// Freeing a large file can be slow. Keep its identity registered, but do not serialize
    /// other regions' destruction behind the page release, including for non-OS memory sources.
    if (region_fd != -1)
        releasePagesNoThrow(region_fd);

    {
        std::lock_guard lock(region_registry_mutex);
        if (previous_region)
            previous_region->next_region = next_region;
        else
            first_region = next_region;
        if (next_region)
            next_region->previous_region = previous_region;

        if (region_data)
            unmapNoThrow(region_data, region_size, "destruction");
    }

    if (region_fd != -1)
        closeNoThrow(region_fd, "destruction");
}

#else

/// Not Linux: no `memfd_create`, no sealing, no `/proc/self/fd`. The loader refuses a function
/// that asks for the transport by calling `checkSupported`, so nothing below runs; it only has
/// to compile.

void SharedMemoryRegion::checkSupported(bool)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Shared-memory regions for executable UDFs are supported only on Linux");
}

#if HAS_MEMORY_STATISTICS_OS
uint64_t SharedMemoryRegion::residentMemoryWithoutRegions(const MemoryStatisticsOS & statistics)
{
    return statistics.get().resident;
}
#endif

SharedMemoryRegion::SharedMemoryRegion(size_t)
{
    checkSupported();
}

void SharedMemoryRegion::grow(size_t)
{
    checkSupported();
}

size_t SharedMemoryRegion::releasePagesUpToLength()
{
    checkSupported();
    return 0;
}

void SharedMemoryRegion::recommitUpToLength()
{
    checkSupported();
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
