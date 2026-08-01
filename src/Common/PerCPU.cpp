#include <Common/PerCPU.h>

#if defined(OS_LINUX)
#include <sys/sysinfo.h>
#include <sys/syscall.h>
#include <sched.h>
#include <unistd.h>
#include <cstddef>

/// The rseq area location of the initial registration, exported by glibc >= 2.35 (Feb 2022;
/// e.g. RHEL 9 and Ubuntu 20.04 ship older). Declared weak
/// so the binary also links against older/other libcs, where the address resolves to null.
/// `__rseq_size` is 0 when registration was disabled (the `glibc.pthread.rseq` tunable) or failed.
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wreserved-identifier"
extern "C" const ptrdiff_t __rseq_offset __attribute__((weak)); // NOLINT(bugprone-reserved-identifier,cert-dcl37-c,cert-dcl51-cpp)
extern "C" const unsigned int __rseq_size __attribute__((weak)); // NOLINT(bugprone-reserved-identifier,cert-dcl37-c,cert-dcl51-cpp)
#pragma clang diagnostic pop

/// The syscall exists since kernel 4.18 (Aug 2018, RHEL 8's baseline), but the kernel headers in
/// the build sysroots predate it.
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wreserved-macro-identifier"
#if !defined(__NR_rseq)
    #if defined(__x86_64__)
        #define __NR_rseq 334
    #elif defined(__aarch64__) || defined(__riscv) || defined(__loongarch64)
        #define __NR_rseq 293
    #elif defined(__powerpc64__)
        #define __NR_rseq 387
    #elif defined(__s390x__)
        #define __NR_rseq 383
    #endif
#endif
#pragma clang diagnostic pop

#elif defined(OS_DARWIN) || defined(OS_FREEBSD)
#include <unistd.h>
#endif

#include <algorithm>

namespace PerCPU
{

UInt32 getNumCPUs() noexcept
{
    static const UInt32 cached = []
    {
#if defined(OS_LINUX)
        const Int64 n = get_nprocs_conf();
#elif defined(OS_DARWIN) || defined(OS_FREEBSD)
        const Int64 n = ::sysconf(_SC_NPROCESSORS_ONLN);
#else
        const Int64 n = 1;
#endif
        if (n <= 0)
            return UInt32{1};
        return std::min(static_cast<UInt32>(n), MAX_CPUS);
    }();
    return cached;
}

#if defined(OS_LINUX)

namespace
{

/// The original v4.18 rseq ABI (include/uapi/linux/rseq.h; bundled because the sysroot headers
/// predate it). Kernels before 6.3 accept only this exact 32-byte size for registration; the 6.3
/// extension fields (node_id, mm_cid) are not needed, would sit in the trailing padding, and are
/// never written for a 32-byte registration. The kernel rewrites the id fields on every
/// migration/preemption of the registered thread, hence volatile.
struct KernelRSeq
{
    volatile UInt32 cpu_id_start;
    volatile UInt32 cpu_id;
    /// Inside the registered area, required zero by the ABI; unused (no rseq critical sections).
    UInt64 rseq_cs;
    UInt32 flags;
} __attribute__((aligned(32)));

static_assert(sizeof(KernelRSeq) == 32);

/// Kernel sentinels in `cpu_id`, never real ids; they double as this thread's registration state
/// for `self_rseq`: -1 = not registered (yet), -2 = registration attempted and failed.
constexpr Int32 CPU_ID_UNINITIALIZED = -1;
constexpr Int32 CPU_ID_REGISTRATION_FAILED = -2;

/// Signature the kernel validates rseq critical-section abort handlers against, fixed at
/// registration. We never enter a critical section, but registering with the canonical
/// glibc/librseq value keeps the area usable for them.
[[maybe_unused]] constexpr UInt32 RSEQ_SIG = 0x53053053;

/// Our own per-thread area, registered lazily when glibc did not register one. Constant-
/// initialized static TLS: no init guard, safe from any context. Never unregistered: the
/// TLS block outlives the thread, and the kernel only writes while the thread runs.
thread_local KernelRSeq self_rseq __attribute__((tls_model("initial-exec"))) =
{
    .cpu_id_start = 0,
    .cpu_id = static_cast<UInt32>(CPU_ID_UNINITIALIZED),
    .rseq_cs = 0,
    .flags = 0,
};

/// glibc 2.35+ registers every thread at creation and exports the area location; then rseq is
/// glibc's and we must only read. When the symbols are absent (older glibc at runtime) or
/// `__rseq_size` is 0 (registration disabled or failed) glibc will never register, and rseq is
/// free for us to take — the `glibc.pthread.rseq` tunable is documented as leaving rseq to the
/// application.
ALWAYS_INLINE bool glibcManagesRSeq()
{
    /// The registered area must cover at least the `cpu_id` field.
    return &__rseq_size != nullptr && __rseq_size >= offsetof(KernelRSeq, cpu_id) + sizeof(UInt32);
}

ALWAYS_INLINE Int32 glibcRSeqCPUID()
{
    const char * tp = static_cast<const char *>(__builtin_thread_pointer());
    return static_cast<Int32>(*reinterpret_cast<const volatile UInt32 *>(tp + __rseq_offset + offsetof(KernelRSeq, cpu_id)));
}

/// One registration attempt per thread (a single syscall); the kernel fills `cpu_id` before the
/// syscall returns. Failure — kernel < 4.18 or CONFIG_RSEQ off (ENOSYS), a seccomp filter
/// (EPERM), another in-process registration (EBUSY) — is recorded in `cpu_id`, sending this
/// thread to the fallback forever.
Int32 registerSelfRSeq() noexcept
{
#if defined(__NR_rseq)
    if (0 == ::syscall(__NR_rseq, &self_rseq, sizeof(self_rseq), 0, RSEQ_SIG))
        return static_cast<Int32>(self_rseq.cpu_id);
#endif
    self_rseq.cpu_id = static_cast<UInt32>(CPU_ID_REGISTRATION_FAILED);
    return ::sched_getcpu();
}

}

namespace detail
{

ALWAYS_INLINE Int32 getCurrentCPULinux() noexcept
{
    if (glibcManagesRSeq())
    {
        const Int32 cpu = glibcRSeqCPUID();
        if (likely(cpu >= 0))
            return cpu;
        /// Negative sentinel: this thread's kernel registration failed; glibc treats that as
        /// non-fatal, and so do we.
        return ::sched_getcpu();
    }

    const Int32 cpu = static_cast<Int32>(self_rseq.cpu_id);
    if (likely(cpu >= 0))
        return cpu;
    if (cpu == CPU_ID_UNINITIALIZED)
        return registerSelfRSeq();
    return ::sched_getcpu();
}

}

#endif

bool haveRSeq() noexcept
{
#if defined(OS_LINUX)
    if (glibcManagesRSeq())
        return glibcRSeqCPUID() >= 0;
    if (static_cast<Int32>(self_rseq.cpu_id) == CPU_ID_UNINITIALIZED)
        registerSelfRSeq();
    return static_cast<Int32>(self_rseq.cpu_id) >= 0;
#else
    return false;
#endif
}

}
