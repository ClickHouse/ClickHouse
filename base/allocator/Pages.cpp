#include <allocator/Pages.h>

#include <allocator/Format.h>
#include <allocator/SizeClassConstants.h>

#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

#if defined(__linux__)
#    include <sys/prctl.h>
#    ifndef PR_SET_VMA
#        define PR_SET_VMA 0x53564d41
#        define PR_SET_VMA_ANON_NAME 0
#    endif
#endif

#if defined(__FreeBSD__)
#    include <sys/sysctl.h>
#    include <vm/vm_param.h>
#endif

#if defined(__APPLE__)
#    include <mach/vm_statistics.h>
#endif

namespace jemalloc
{

/// --- Data ----------------------------------------------------------------------------------------------------------

constinit size_t os_page = 0;

constinit const char * const thp_mode_names[] = {"default", "always", "never", "not supported"};
constinit const char * const system_thp_mode_names[] = {"madvise", "always", "never", "not supported"};

constinit const char * const metadata_thp_mode_names[] = {"disabled", "auto", "always"};

constinit SystemThpMode init_system_thp_mode = SystemThpMode::Madvise;

namespace pages
{

namespace
{

constexpr int PAGES_PROT_COMMIT = PROT_READ | PROT_WRITE;
constexpr int PAGES_PROT_DECOMMIT = PROT_NONE;

/// jemalloc: PAGES_FD_TAG
#if defined(__APPLE__)
const int PAGES_FD_TAG = VM_MAKE_TAG(254U);
#else
constexpr int PAGES_FD_TAG = -1;
#endif

constinit int mmap_flags = 0;
constinit bool os_overcommits = false;

/// Runtime support for lazy purge. Irrelevant when `!can_purge_lazy`.
/// jemalloc: pages_can_purge_lazy_runtime
constinit bool pages_can_purge_lazy_runtime = true;

/// -1 until `boot` ran the probe: treated as faulty (forced purge fails) before that.
/// jemalloc: madvise_dont_need_zeros_is_faulty
constinit int madvise_dont_need_zeros_is_faulty = -1;

/// `MADV_FREE` (the ppc64le headers may lack it: `JEMALLOC_DEFINE_MADVISE_FREE` uses 8).
#if defined(MADV_FREE)
constexpr int MADVISE_FREE = MADV_FREE;
#else
constexpr int MADVISE_FREE = 8;
#endif

void osPagesUnmap(void * addr, size_t size);

/// Check that `MADV_DONTNEED` will actually zero pages on subsequent access (it does not under QEMU).
/// jemalloc: madvise_MADV_DONTNEED_zeroes_pages
int madviseDontNeedZeroesPages()
{
    size_t size = PAGE;

    void * addr = ::mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);

    if (addr == MAP_FAILED)
    {
        writeMessage("<jemalloc>: Cannot allocate memory for MADV_DONTNEED check\n");
        /// jemalloc compatibility: jemalloc continues only if `!opt_abort` and then writes to `MAP_FAILED`, which
        /// kills the process; abort right away instead.
        abort();
    }

    memset(addr, 'A', size);
    int works;
    if (::madvise(addr, size, MADV_DONTNEED) == 0)
        works = memchr(addr, 'A', size) == nullptr;
    else
    {
        /// If madvise() does not support MADV_DONTNEED, then we can call it anyway, and use its return code.
        works = 1;
    }

    if (::munmap(addr, size) != 0)
    {
        writeMessage("<jemalloc>: Cannot deallocate memory for MADV_DONTNEED check\n");
        if (opt.abort)
            abort();
    }

    return works;
}

/// Name the mapping in `/proc/<pid>/maps`, e.g. `7f4836000000-7f4836800000 rw-p 00000000 00:00 0
/// [anon:jemalloc_pg_overcommit]`. Errors are ignored (EINVAL on kernels without `CONFIG_ANON_VMA_NAME`).
/// jemalloc: os_page_id
[[maybe_unused]] int osPageId(void * addr, size_t size, const char * name)
{
#if defined(__linux__)
    JE_ASSERT(addr != nullptr);
    int n = ::prctl(PR_SET_VMA, PR_SET_VMA_ANON_NAME, reinterpret_cast<uintptr_t>(addr), size, reinterpret_cast<uintptr_t>(name));
    JE_ASSERT(n == 0 || (n == -1 && errno == EINVAL));
    return n;
#else
    (void)addr;
    (void)size;
    (void)name;
    return 0;
#endif
}

/// jemalloc: os_pages_map
void * osPagesMap(void * addr, size_t size, size_t /*alignment*/, bool * commit)
{
    JE_ASSERT(alignmentAddrToBase(addr, os_page) == addr);
    JE_ASSERT(alignmentCeiling(size, os_page) == size);
    JE_ASSERT(size != 0);

    if (os_overcommits)
        *commit = true;

    /// We don't use MAP_FIXED here, because it can cause the *replacement* of existing mappings, and we only want
    /// to create new mappings.
    int prot = *commit ? PAGES_PROT_COMMIT : PAGES_PROT_DECOMMIT;
    void * ret = ::mmap(addr, size, prot, mmap_flags, PAGES_FD_TAG, 0);
    JE_ASSERT(ret != nullptr);

    if (ret == MAP_FAILED)
        ret = nullptr;
    else if (addr != nullptr && ret != addr)
    {
        /// We succeeded in mapping memory, but not in the right place.
        osPagesUnmap(ret, size);
        ret = nullptr;
    }
    JE_ASSERT(ret == nullptr || (addr == nullptr && ret != addr) || (addr != nullptr && ret == addr));

    if constexpr (config::pageid)
    {
        if (ret != nullptr)
            osPageId(ret, size, os_overcommits ? "jemalloc_pg_overcommit" : "jemalloc_pg");
    }
    return ret;
}

/// jemalloc: os_pages_trim
void * osPagesTrim(void * addr, size_t alloc_size, size_t leadsize, size_t size, bool * /*commit*/)
{
    void * ret = static_cast<char *>(addr) + leadsize;

    JE_ASSERT(alloc_size >= leadsize + size);
    size_t trailsize = alloc_size - leadsize - size;

    if (leadsize != 0)
        osPagesUnmap(addr, leadsize);
    if (trailsize != 0)
        osPagesUnmap(static_cast<char *>(ret) + size, trailsize);
    return ret;
}

/// jemalloc: os_pages_unmap
void osPagesUnmap(void * addr, size_t size)
{
    JE_ASSERT(alignmentAddrToBase(addr, os_page) == addr);
    JE_ASSERT(alignmentCeiling(size, os_page) == size);

    if (::munmap(addr, size) == -1)
    {
        char buf[BUFERROR_BUF];

        bufferError(errno, buf, sizeof(buf));
        printMessage("<jemalloc>: Error in munmap(): %s\n", buf);
        if (opt.abort)
            abort();
    }
}

/// jemalloc: pages_map_slow
void * pagesMapSlow(size_t size, size_t alignment, bool * commit)
{
    size_t alloc_size = size + alignment - os_page;
    /// Beware size_t wrap-around.
    if (alloc_size < size)
        return nullptr;

    void * ret;
    do
    {
        void * pages = osPagesMap(nullptr, alloc_size, alignment, commit);
        if (pages == nullptr)
            return nullptr;
        size_t leadsize = alignmentCeiling(reinterpret_cast<uintptr_t>(pages), alignment) - reinterpret_cast<uintptr_t>(pages);
        ret = osPagesTrim(pages, alloc_size, leadsize, size, commit);
    } while (ret == nullptr);

    JE_ASSERT(ret != nullptr);
    JE_ASSERT(pageAddrToBase(ret) == ret);
    return ret;
}

/// jemalloc: os_pages_commit
bool osPagesCommit(void * addr, size_t size, bool commit)
{
    JE_ASSERT(pageAddrToBase(addr) == addr);
    JE_ASSERT(pageCeiling(size) == size);

    int prot = commit ? PAGES_PROT_COMMIT : PAGES_PROT_DECOMMIT;
    void * result = ::mmap(addr, size, prot, mmap_flags | MAP_FIXED, PAGES_FD_TAG, 0);
    if (result == MAP_FAILED)
        return true;
    if (result != addr)
    {
        /// We succeeded in mapping memory, but not in the right place.
        osPagesUnmap(result, size);
        return true;
    }
    return false;
}

/// jemalloc: pages_commit_impl
bool pagesCommitImpl(void * addr, size_t size, bool commit)
{
    if (os_overcommits)
        return true;

    return osPagesCommit(addr, size, commit);
}

/// jemalloc: pages_huge_impl
bool pagesHugeImpl(void * addr, size_t size, bool aligned)
{
    if (aligned)
    {
        JE_ASSERT(hugepageAddrToBase(addr) == addr);
        JE_ASSERT(hugepageCeiling(size) == size);
    }
#if defined(__linux__)
    return ::madvise(addr, size, MADV_HUGEPAGE) != 0;
#else
    (void)addr;
    (void)size;
    return true;
#endif
}

/// jemalloc: pages_huge_unaligned
bool pagesHugeUnaligned(void * addr, size_t size)
{
    return pagesHugeImpl(addr, size, false);
}

/// jemalloc: pages_nohuge_impl
bool pagesNohugeImpl(void * addr, size_t size, bool aligned)
{
    if (aligned)
    {
        JE_ASSERT(hugepageAddrToBase(addr) == addr);
        JE_ASSERT(hugepageCeiling(size) == size);
    }
#if defined(__linux__)
    return ::madvise(addr, size, MADV_NOHUGEPAGE) != 0;
#else
    (void)addr;
    (void)size;
    return false;
#endif
}

/// jemalloc: pages_nohuge_unaligned
bool pagesNohugeUnaligned(void * addr, size_t size)
{
    return pagesNohugeImpl(addr, size, false);
}

/// jemalloc: os_page_detect
size_t osPageDetect()
{
#if defined(__FreeBSD__)
    /// This returns the value obtained from the auxv vector, avoiding a syscall.
    return static_cast<size_t>(getpagesize());
#else
    long result = sysconf(_SC_PAGESIZE);
    if (result == -1)
    {
        /// jemalloc compatibility: returns `LG_PAGE` rather than `PAGE` (an upstream oddity).
        return LG_PAGE;
    }
    return static_cast<size_t>(result);
#endif
}

#if defined(__FreeBSD__)
/// jemalloc: os_overcommits_sysctl
bool osOvercommitsSysctl()
{
    int vm_overcommit;
    size_t sz = sizeof(vm_overcommit);
#    if defined(VM_OVERCOMMIT)
    int mib[2];
    mib[0] = CTL_VM;
    mib[1] = VM_OVERCOMMIT;
    if (::sysctl(mib, 2, &vm_overcommit, &sz, nullptr, 0) != 0)
        return false; /// Error.
#    else
    if (::sysctlbyname("vm.overcommit", &vm_overcommit, &sz, nullptr, 0) != 0)
        return false; /// Error.
#    endif
    return (vm_overcommit & 0x3) == 0;
}
#endif

/// jemalloc: os_overcommits_proc
[[maybe_unused]] bool osOvercommitsProc()
{
    int fd;
    char buf[1];

    fd = openFile("/proc/sys/vm/overcommit_memory", O_RDONLY | O_CLOEXEC);
    if (fd == -1)
        return false; /// Error.

    ssize_t nread = readFd(fd, &buf, sizeof(buf));
    closeFile(fd);

    if (nread < 1)
        return false; /// Error.

    /// /proc/sys/vm/overcommit_memory meanings:
    /// 0: Heuristic overcommit.
    /// 1: Always overcommit.
    /// 2: Never overcommit.
    return buf[0] == '0' || buf[0] == '1';
}

/// jemalloc: pages_should_skip_set_thp_state
bool pagesShouldSkipSetThpState()
{
    return opt.thp == ThpMode::DoNothing || (opt.thp == ThpMode::Always && init_system_thp_mode == SystemThpMode::Always)
        || (opt.thp == ThpMode::Never && init_system_thp_mode == SystemThpMode::Never);
}

}

namespace
{

/// jemalloc: init_thp_state
void initThpState()
{
    if constexpr (!config::have_madvise_huge)
    {
        if (metadataThpEnabled() && opt.abort)
        {
            writeMessage("<jemalloc>: no MADV_HUGEPAGE support\n");
            abort();
        }
    }
    else
    {
        static constexpr char sys_state_madvise[] = "always [madvise] never\n";
        static constexpr char sys_state_always[] = "[always] madvise never\n";
        static constexpr char sys_state_never[] = "always madvise [never]\n";
        char buf[sizeof(sys_state_madvise)];

        int fd = openFile("/sys/kernel/mm/transparent_hugepage/enabled", O_RDONLY);
        if (fd != -1)
        {
            ssize_t nread = readFd(fd, &buf, sizeof(buf));
            closeFile(fd);
            if (nread >= 0)
            {
                if (strncmp(buf, sys_state_madvise, static_cast<size_t>(nread)) == 0)
                {
                    init_system_thp_mode = SystemThpMode::Madvise;
                    return;
                }
                if (strncmp(buf, sys_state_always, static_cast<size_t>(nread)) == 0)
                {
                    init_system_thp_mode = SystemThpMode::Always;
                    return;
                }
                if (strncmp(buf, sys_state_never, static_cast<size_t>(nread)) == 0)
                {
                    init_system_thp_mode = SystemThpMode::Never;
                    return;
                }
                /// `opt_hpa_opts.hugify_style` adjustments are dropped together with HPA.
            }
        }
    }

    /// label_error:
    opt.thp = ThpMode::NotSupported;
    init_system_thp_mode = SystemThpMode::NotSupported;
}

}

/// --- Mapping ---------------------------------------------------------------------------------------------------

/// jemalloc: pages_map
void * map(void * addr, size_t size, size_t alignment, bool * commit)
{
    JE_ASSERT(alignment >= PAGE);
    JE_ASSERT(alignmentAddrToBase(addr, alignment) == addr);

#if defined(__FreeBSD__) && defined(MAP_EXCL)
    /// FreeBSD has mechanisms both to mmap at specific address without touching existing mappings, and to mmap with
    /// specific alignment.
    {
        if (os_overcommits)
            *commit = true;

        int prot = *commit ? PAGES_PROT_COMMIT : PAGES_PROT_DECOMMIT;
        int flags = mmap_flags;

        if (addr != nullptr)
            flags |= MAP_FIXED | MAP_EXCL;
        else
        {
            /// jemalloc: `ffs_zu(alignment)` (0-based: log2 of the alignment).
            unsigned alignment_bits = ffs(alignment);
            JE_ASSERT(alignment_bits > 0);
            flags |= MAP_ALIGNED(alignment_bits);
        }

        void * ret = ::mmap(addr, size, prot, flags, -1, 0);
        if (ret == MAP_FAILED)
            ret = nullptr;

        return ret;
    }
#endif
    /// Ideally, there would be a way to specify alignment to mmap() (like NetBSD has), but in the absence of such a
    /// feature, we have to work hard to efficiently create aligned mappings. The reliable, but slow method is to
    /// create a mapping that is over-sized, then trim the excess. However, that always results in one or two calls
    /// to `osPagesUnmap`, and it can leave holes in the process's virtual memory map if memory grows downward.
    ///
    /// Optimistically try mapping precisely the right amount before falling back to the slow method, with the
    /// expectation that the optimistic approach works most of the time.
    void * ret = osPagesMap(addr, size, os_page, commit);
    if (ret == nullptr || ret == addr)
        return ret;
    JE_ASSERT(addr == nullptr);
    if (alignmentAddrToOffset(reinterpret_cast<uintptr_t>(ret), alignment) != 0)
    {
        osPagesUnmap(ret, size);
        return pagesMapSlow(size, alignment, commit);
    }

    JE_ASSERT(pageAddrToBase(ret) == ret);
    return ret;
}

/// jemalloc: pages_unmap
void unmap(void * addr, size_t size)
{
    JE_ASSERT(pageAddrToBase(addr) == addr);
    JE_ASSERT(pageCeiling(size) == size);

    osPagesUnmap(addr, size);
}

/// jemalloc: pages_commit
bool commit(void * addr, size_t size)
{
    return pagesCommitImpl(addr, size, true);
}

/// jemalloc: pages_decommit
bool decommit(void * addr, size_t size)
{
    return pagesCommitImpl(addr, size, false);
}

/// jemalloc: pages_mark_guards (`JEMALLOC_HAVE_MPROTECT` is defined on all supported platforms)
void markGuards(void * head, void * tail)
{
    JE_ASSERT(head != nullptr || tail != nullptr);
    JE_ASSERT(head == nullptr || tail == nullptr || reinterpret_cast<uintptr_t>(head) < reinterpret_cast<uintptr_t>(tail));
    if (head != nullptr)
        ::mprotect(head, PAGE, PROT_NONE);
    if (tail != nullptr)
        ::mprotect(tail, PAGE, PROT_NONE);
}

/// jemalloc: pages_unmark_guards
void unmarkGuards(void * head, void * tail)
{
    JE_ASSERT(head != nullptr || tail != nullptr);
    JE_ASSERT(head == nullptr || tail == nullptr || reinterpret_cast<uintptr_t>(head) < reinterpret_cast<uintptr_t>(tail));
    bool head_and_tail = (head != nullptr) && (tail != nullptr);
    size_t range = head_and_tail ? reinterpret_cast<uintptr_t>(tail) - reinterpret_cast<uintptr_t>(head) + PAGE : SIZE_MAX;
    /// The amount of work that the kernel does in mprotect depends on the range argument. SC_LARGE_MINCLASS is an
    /// arbitrary threshold chosen to prevent kernel from doing too much work that would outweigh the savings of
    /// performing one less system call.
    bool ranged_mprotect = head_and_tail && range <= SC_LARGE_MINCLASS;
    if (ranged_mprotect)
        ::mprotect(head, range, PROT_READ | PROT_WRITE);
    else
    {
        if (head != nullptr)
            ::mprotect(head, PAGE, PROT_READ | PROT_WRITE);
        if (tail != nullptr)
            ::mprotect(tail, PAGE, PROT_READ | PROT_WRITE);
    }
}

/// --- Purging ---------------------------------------------------------------------------------------------------

/// jemalloc: pages_purge_lazy
bool purgeLazy(void * addr, size_t size)
{
    JE_ASSERT(alignmentAddrToBase(addr, os_page) == addr);
    JE_ASSERT(pageCeiling(size) == size);

    if constexpr (!can_purge_lazy)
        return true;
    if (!pages_can_purge_lazy_runtime)
    {
        /// Built with lazy purge enabled, but detected it was not supported on the current system.
        return true;
    }

    return ::madvise(addr, size, MADVISE_FREE) != 0;
}

/// jemalloc: pages_purge_forced
bool purgeForced(void * addr, size_t size)
{
    JE_ASSERT(pageAddrToBase(addr) == addr);
    JE_ASSERT(pageCeiling(size) == size);

    if constexpr (!can_purge_forced)
        return true;

    if constexpr (config::purge_madvise_dontneed_zeros)
        return JE_UNLIKELY(madvise_dont_need_zeros_is_faulty) || ::madvise(addr, size, MADV_DONTNEED) != 0;
    else
    {
        /// `JEMALLOC_MAPS_COALESCE`: try to overlay a new demand-zeroed mapping.
        return commit(addr, size);
    }
}

/// --- Hugepages and dumps ---------------------------------------------------------------------------------------

/// jemalloc: pages_huge
bool huge(void * addr, size_t size)
{
    return pagesHugeImpl(addr, size, true);
}

/// jemalloc: pages_nohuge
bool nohuge(void * addr, size_t size)
{
    return pagesNohugeImpl(addr, size, true);
}

/// jemalloc: pages_collapse (`JEMALLOC_HAVE_MADVISE_COLLAPSE` is not defined on any supported platform)
bool collapse(void * addr, size_t size)
{
    JE_ASSERT(pageAddrToBase(addr) == addr);
    JE_ASSERT(pageCeiling(size) == size);
    (void)addr;
    (void)size;
    return true;
}

/// jemalloc: pages_dontdump
bool dontDump(void * addr, size_t size)
{
    JE_ASSERT(pageAddrToBase(addr) == addr);
    JE_ASSERT(pageCeiling(size) == size);
#if defined(__linux__)
    return ::madvise(addr, size, MADV_DONTDUMP) != 0;
#elif defined(__FreeBSD__)
    return ::madvise(addr, size, MADV_NOCORE) != 0;
#else
    (void)addr;
    (void)size;
    return false;
#endif
}

/// jemalloc: pages_dodump
bool doDump(void * addr, size_t size)
{
    JE_ASSERT(pageAddrToBase(addr) == addr);
    JE_ASSERT(pageCeiling(size) == size);
#if defined(__linux__)
    return ::madvise(addr, size, MADV_DODUMP) != 0;
#elif defined(__FreeBSD__)
    return ::madvise(addr, size, MADV_CORE) != 0;
#else
    (void)addr;
    (void)size;
    return false;
#endif
}

/// jemalloc: pages_set_thp_state
void setThpState(void * ptr, size_t size)
{
    if (pagesShouldSkipSetThpState())
        return;
    JE_ASSERT(opt.thp != ThpMode::NotSupported && init_system_thp_mode != SystemThpMode::NotSupported);

    if (opt.thp == ThpMode::Always && init_system_thp_mode == SystemThpMode::Madvise)
        pagesHugeUnaligned(ptr, size);
    else if (opt.thp == ThpMode::Never)
    {
        JE_ASSERT(init_system_thp_mode == SystemThpMode::Madvise || init_system_thp_mode == SystemThpMode::Always);
        pagesNohugeUnaligned(ptr, size);
    }
}

/// --- Boot ------------------------------------------------------------------------------------------------------

/// jemalloc: pages_boot
bool boot()
{
    os_page = osPageDetect();
    if (os_page > PAGE)
    {
        writeMessage("<jemalloc>: Unsupported system page size\n");
        if (opt.abort)
            abort();
        return true;
    }

    if constexpr (config::purge_madvise_dontneed_zeros)
    {
        if (!opt.trust_madvise)
        {
            madvise_dont_need_zeros_is_faulty = !madviseDontNeedZeroesPages();
            if (madvise_dont_need_zeros_is_faulty)
            {
                writeMessage("<jemalloc>: MADV_DONTNEED does not work (memset will be used instead)\n");
                writeMessage("<jemalloc>: (This is the expected behaviour if you are running under QEMU)\n");
            }
        }
        else
        {
            /// In case `opt.trust_madvise` is enabled, do not do the runtime check.
            madvise_dont_need_zeros_is_faulty = 0;
        }
    }

    mmap_flags = MAP_PRIVATE | MAP_ANON;

#if defined(__FreeBSD__)
    os_overcommits = osOvercommitsSysctl();
#elif defined(__linux__)
    os_overcommits = osOvercommitsProc();
    if (os_overcommits)
        mmap_flags |= MAP_NORESERVE;
#else
    os_overcommits = false;
#endif

    initThpState();

    if constexpr (!config::os_freebsd)
    {
        /// Detect lazy purge runtime support (FreeBSD doesn't need the check; madvise(2) is known to work).
        if constexpr (can_purge_lazy)
        {
            bool committed = false;
            void * madv_free_page = osPagesMap(nullptr, PAGE, PAGE, &committed);
            if (madv_free_page == nullptr)
                return true;
            JE_ASSERT(pages_can_purge_lazy_runtime);
            if (purgeLazy(madv_free_page, PAGE))
                pages_can_purge_lazy_runtime = false;
            osPagesUnmap(madv_free_page, PAGE);
        }
    }

    /// `init_process_madvise` is dead (`JEMALLOC_HAVE_PROCESS_MADVISE` is not configured) and returns false.
    return false;
}

bool osOvercommits()
{
    return os_overcommits;
}

int mmapFlags()
{
    return mmap_flags;
}

bool madviseDontNeedZerosIsFaulty()
{
    return madvise_dont_need_zeros_is_faulty != 0;
}

bool canPurgeLazyRuntime()
{
    return pages_can_purge_lazy_runtime;
}

}

/// --- extent_mmap.c ---------------------------------------------------------------------------------------------

/// jemalloc: extent_alloc_mmap
void * extentAllocMmap(void * new_addr, size_t size, size_t alignment, bool * zero, bool * commit)
{
    JE_ASSERT(alignment == alignmentCeiling(alignment, PAGE));
    void * ret = pages::map(new_addr, size, alignment, commit);
    if (ret == nullptr)
        return nullptr;
    if (*commit)
        *zero = true;
    return ret;
}

/// jemalloc: extent_dalloc_mmap
bool extentDallocMmap(void * addr, size_t size)
{
    if (!opt.retain)
        pages::unmap(addr, size);
    return opt.retain;
}

}
