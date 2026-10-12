#pragma once

/// Build and platform configuration of the allocator.
///
/// Replaces jemalloc's `jemalloc_internal_defs.h` + `jemalloc_preamble.h`: every platform knob is a `constexpr`
/// constant here, so the rest of the code uses `if constexpr` instead of `#ifdef` (dead branches are still discarded).
/// The values reproduce the per-platform configuration of `contrib/jemalloc-cmake/include_<os>_<arch>`.
///
/// Inputs from the build system:
///   ALLOCATOR_LG_PAGE          - log2 of the allocator page size (only required on Linux aarch64, where it is configurable).
///   ALLOCATOR_MALLOC_CONF      - the compiled-in configuration string (`config.malloc_conf`).
///   ALLOCATOR_UAF_DETECTION    - 1 to compile in use-after-free detection (`ENABLE_JEMALLOC_UAF_SAN`).
///   ALLOCATOR_MUSL             - 1 when building against musl libc.
///   ALLOCATOR_DEBUG            - 1 to enable internal assertions (development only; jemalloc never enables them in ClickHouse).

#include <cstddef>
#include <cstdint>

namespace jemalloc
{

enum class OS : uint8_t
{
    Linux,
    FreeBSD,
    Darwin,
};

enum class Arch : uint8_t
{
    X86_64,
    AArch64,
    PPC64LE,
    RISCV64,
    S390X,
};

/// How thread-specific data is implemented (jemalloc: `tsd_tls.h`, `tsd_malloc_thread_cleanup.h`, `tsd_generic.h`).
enum class ThreadStateImpl : uint8_t
{
    /// `thread_local` (initial-exec) + a pthread key whose destructor cleans up. Linux.
    TLS,
    /// `thread_local` + libc's `_malloc_thread_cleanup` hook. FreeBSD.
    MallocThreadCleanup,
    /// `pthread_getspecific` with an allocated wrapper. Darwin.
    Generic,
};

namespace config
{

#if defined(__linux__)
inline constexpr OS os = OS::Linux;
#elif defined(__FreeBSD__)
inline constexpr OS os = OS::FreeBSD;
#elif defined(__APPLE__)
inline constexpr OS os = OS::Darwin;
#else
#error "Unsupported OS"
#endif

#if defined(__x86_64__)
inline constexpr Arch arch = Arch::X86_64;
#elif defined(__aarch64__)
inline constexpr Arch arch = Arch::AArch64;
#elif defined(__powerpc64__)
inline constexpr Arch arch = Arch::PPC64LE;
#elif defined(__riscv) && __riscv_xlen == 64
inline constexpr Arch arch = Arch::RISCV64;
#elif defined(__s390x__)
inline constexpr Arch arch = Arch::S390X;
#else
#error "Unsupported architecture"
#endif

#if defined(ALLOCATOR_MUSL) && ALLOCATOR_MUSL
inline constexpr bool musl = true;
#else
inline constexpr bool musl = false;
#endif

inline constexpr bool os_linux = os == OS::Linux;
inline constexpr bool os_freebsd = os == OS::FreeBSD;
inline constexpr bool os_darwin = os == OS::Darwin;

/// --- Geometry ---------------------------------------------------------------------------------------------------

/// LOG2_PAGE: the allocator's page size, which must not be smaller than the kernel page size.
#if defined(ALLOCATOR_LG_PAGE)
inline constexpr unsigned log2_page = ALLOCATOR_LG_PAGE;
#elif defined(__linux__) && (defined(__x86_64__) || defined(__s390x__))
inline constexpr unsigned log2_page = 12;
#elif defined(__linux__) && (defined(__powerpc64__) || defined(__riscv))
inline constexpr unsigned log2_page = 16;
#elif defined(__linux__) && defined(__aarch64__)
#error "ALLOCATOR_LG_PAGE must be specified on Linux aarch64 (JEMALLOC_AARCH64_PAGE_SIZE_KIB)"
#elif defined(__FreeBSD__) && defined(__aarch64__)
inline constexpr unsigned log2_page = 16;
#elif defined(__FreeBSD__)
inline constexpr unsigned log2_page = 12;
#elif defined(__APPLE__) && defined(__aarch64__)
inline constexpr unsigned log2_page = 14;
#elif defined(__APPLE__)
inline constexpr unsigned log2_page = 12;
#endif

static_assert(log2_page == 12 || log2_page == 14 || log2_page == 16, "Unsupported page size");

/// LOG2_HUGE_PAGE.
inline constexpr unsigned log2_huge_page = []
{
    if constexpr (os_linux && arch == Arch::AArch64)
        return 2 * log2_page - 3; /// A PMD-level THP maps (page size / 8) entries of one page each.
    else if constexpr (os_linux && arch == Arch::RISCV64)
        return 29u;
    else if constexpr (os_linux && arch == Arch::S390X)
        return 20u;
    else if constexpr (os_freebsd && arch == Arch::AArch64)
        return 29u;
    else
        return 21u;
}();

/// LOG2_VIRTUAL_ADDRESS: number of significant virtual address bits.
inline constexpr unsigned log2_virtual_address = []
{
    if constexpr (arch == Arch::PPC64LE || arch == Arch::S390X)
        return 64u;
    else if constexpr (os_darwin && arch == Arch::AArch64)
        return 64u;
    else
        return 48u;
}();

inline constexpr unsigned log2_sizeof_ptr = 3;
inline constexpr unsigned log2_quantum = 4;
inline constexpr unsigned log2_cache_line = 6;
inline constexpr bool big_endian = arch == Arch::S390X;

/// --- Features that are the same on every platform -----------------------------------------------------------------

inline constexpr bool debug =
#if defined(ALLOCATOR_DEBUG) && ALLOCATOR_DEBUG
    true;
#else
    false;
#endif

inline constexpr bool stats = true; /// JEMALLOC_STATS
inline constexpr bool fill = true; /// JEMALLOC_FILL
inline constexpr bool profiling = true; /// JEMALLOC_PROF
inline constexpr bool cache_oblivious = true; /// JEMALLOC_CACHE_OBLIVIOUS (default of `opt.cache_oblivious`)
inline constexpr bool maps_coalesce = true; /// JEMALLOC_MAPS_COALESCE
inline constexpr bool background_thread = true; /// JEMALLOC_BACKGROUND_THREAD
inline constexpr bool option_safety_checks = false;
inline constexpr bool option_size_checks = false;

inline constexpr bool use_after_free_detection =
#if defined(ALLOCATOR_UAF_DETECTION) && ALLOCATOR_UAF_DETECTION
    true;
#else
    false;
#endif

/// --- Per-platform features ---------------------------------------------------------------------------------------

/// JEMALLOC_RETAIN: keep unused virtual memory mapped instead of unmapping it.
inline constexpr bool retain = os_linux;

/// JEMALLOC_DSS is compiled in everywhere except Darwin (the allocator never uses sbrk; this only affects reporting).
inline constexpr bool have_sbrk = !os_darwin;

/// JEMALLOC_HAVE_MADVISE_HUGE, JEMALLOC_PURGE_MADVISE_DONTNEED_ZEROS, JEMALLOC_MADVISE_DONTDUMP.
inline constexpr bool have_madvise_huge = os_linux;
inline constexpr bool purge_madvise_dontneed_zeros = os_linux;
inline constexpr bool madvise_dontdump = os_linux;
inline constexpr bool madvise_nocore = os_freebsd;

/// JEMALLOC_PAGEID: name anonymous mappings with prctl(PR_SET_VMA).
inline constexpr bool page_id = os_linux && arch != Arch::S390X;

/// JEMALLOC_PROC_SYS_VM_OVERCOMMIT_MEMORY / JEMALLOC_SYSCTL_VM_OVERCOMMIT.
inline constexpr bool proc_system_vm_overcommit_memory = os_linux;
inline constexpr bool sysctl_vm_overcommit = os_freebsd;

/// JEMALLOC_HAVE_VM_MAKE_TAG.
inline constexpr bool have_vm_make_tag = os_darwin;

/// JEMALLOC_ZONE: integrate as a Darwin malloc zone.
inline constexpr bool zone = os_darwin;

/// JEMALLOC_HAVE_SCHED_GETCPU / JEMALLOC_HAVE_SCHED_SETAFFINITY.
inline constexpr bool have_sched_getcpu = os_linux || (os_freebsd && arch == Arch::PPC64LE);
inline constexpr bool have_sched_setaffinity = have_sched_getcpu;

/// JEMALLOC_PERCPU_ARENA: there is a way to query the current CPU (Darwin uses the fork's `malloc_getcpu`).
inline constexpr bool have_per_cpu_arena = have_sched_getcpu || os_darwin;

/// JEMALLOC_HAVE_PTHREAD_ATFORK (missing on Linux ppc64le).
inline constexpr bool have_pthread_atfork = !(os_linux && arch == Arch::PPC64LE);

/// JEMALLOC_HAVE_PTHREAD_SETNAME_NP / GETNAME_NP / GET_NAME_NP.
inline constexpr bool have_pthread_setname_np = os_linux || (os_freebsd && arch == Arch::PPC64LE);
inline constexpr bool have_pthread_getname_np = (os_linux && !musl) || os_darwin || (os_freebsd && arch == Arch::PPC64LE);
inline constexpr bool have_pthread_get_name_np = os_freebsd;

/// JEMALLOC_HAVE_CLOCK_MONOTONIC (Darwin uses gettimeofday in the ClickHouse fork).
inline constexpr bool have_clock_monotonic = !os_darwin;

/// JEMALLOC_THREADED_INIT, JEMALLOC_MUTEX_INIT_CB, JEMALLOC_LAZY_LOCK.
inline constexpr bool threaded_init = os_linux;
inline constexpr bool mutex_init_callback = os_freebsd;
inline constexpr bool lazy_lock = os_freebsd;

/// The thread-specific data implementation.
inline constexpr ThreadStateImpl thread_state_impl
    = os_freebsd ? ThreadStateImpl::MallocThreadCleanup : (os_darwin ? ThreadStateImpl::Generic : ThreadStateImpl::TLS);

/// JEMALLOC_TLS_MODEL_INITIAL_EXEC is not available on Linux aarch64 musl.
inline constexpr bool tls_model_initial_exec = !(os_linux && musl && arch == Arch::AArch64);

/// JEMALLOC_ZERO_REALLOC_DEFAULT_FREE: `realloc(p, 0)` frees `p`.
inline constexpr bool zero_realloc_default_free = os_linux;

/// JEMALLOC_USE_SYSCALL: use raw syscalls for write/read/open/close.
inline constexpr bool use_syscall = !os_darwin;

/// JEMALLOC_HAVE_SECURE_GETENV / JEMALLOC_HAVE_ISSETUGID.
inline constexpr bool have_secure_getenv = os_linux && arch == Arch::S390X;
inline constexpr bool have_issetugid = os_freebsd || os_darwin;

/// JEMALLOC_STRERROR_R_RETURNS_CHAR_WITH_GNU_SOURCE.
inline constexpr bool strerror_r_returns_char = os_linux && !musl;

/// JEMALLOC_ENABLE_CXX: only affects the `experimental_infallible_new` option.
inline constexpr bool enable_cxx = (os_linux && arch == Arch::S390X) || (os_freebsd && arch == Arch::PPC64LE);

/// JEMALLOC_HAVE_MALLOC_SIZE: export `je_malloc_size`.
inline constexpr bool have_malloc_size = os_darwin;

/// HAVE_CPU_SPINWAIT.
inline constexpr bool have_cpu_spin_wait = arch == Arch::X86_64;

/// JEMALLOC_CONFIG_MALLOC_CONF. s390x and FreeBSD ppc64le hard-code an empty string in the jemalloc configuration.
#if defined(ALLOCATOR_MALLOC_CONF)
inline constexpr const char * malloc_conf_default
    = ((os_linux && arch == Arch::S390X) || (os_freebsd && arch == Arch::PPC64LE)) ? "" : ALLOCATOR_MALLOC_CONF;
#else
inline constexpr const char * malloc_conf_default = "";
#endif

}

/// --- Derived constants ----------------------------------------------------------------------------------------------

inline constexpr unsigned LOG2_PAGE = config::log2_page;
inline constexpr size_t PAGE = size_t(1) << LOG2_PAGE;
inline constexpr size_t PAGE_MASK = PAGE - 1;

inline constexpr unsigned LOG2_HUGE_PAGE = config::log2_huge_page;
inline constexpr size_t HUGE_PAGE = size_t(1) << LOG2_HUGE_PAGE;
inline constexpr size_t HUGE_PAGE_MASK = HUGE_PAGE - 1;

inline constexpr unsigned LOG2_VIRTUAL_ADDRESS = config::log2_virtual_address;

inline constexpr unsigned LG_SIZEOF_PTR = config::log2_sizeof_ptr;

inline constexpr unsigned LOG2_QUANTUM = config::log2_quantum;
inline constexpr size_t QUANTUM = size_t(1) << LOG2_QUANTUM;
inline constexpr size_t QUANTUM_MASK = QUANTUM - 1;

inline constexpr unsigned LOG2_CACHE_LINE = config::log2_cache_line;
inline constexpr size_t CACHE_LINE = size_t(1) << LOG2_CACHE_LINE;
inline constexpr size_t CACHE_LINE_MASK = CACHE_LINE - 1;

/// Maximum number of arenas (MALLOCX_ARENA_BITS).
inline constexpr unsigned MALLOCX_ARENA_BITS = 12;
inline constexpr unsigned MALLOCX_ARENA_LIMIT = (1u << MALLOCX_ARENA_BITS) - 1;

/// Maximum number of explicit thread caches (MALLOCX_THREAD_CACHE_BITS).
inline constexpr unsigned MALLOCX_THREAD_CACHE_BITS = 12;
inline constexpr unsigned MALLOCX_THREAD_CACHE_MAX = (1u << MALLOCX_THREAD_CACHE_BITS) - 3;

}
