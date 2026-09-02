/// Cost of obtaining the current CPU id by different mechanisms:
///   - `sched_getcpu`: whatever the libc in this build provides (rseq TLS read, vDSO, or syscall)
///   - rseq: direct read of `cpu_id` from the libc-registered rseq area (glibc >= 2.35)
///   - vDSO: `__vdso_getcpu` resolved from the vDSO (x86_64 only; other architectures skip)
///   - syscall: raw `SYS_getcpu`
///
/// Mechanisms unavailable in the current environment are reported as skipped rather than
/// silently measuring a fallback.
///
/// Possible results:
///
///     -------------------------------------------------------------------
///     Benchmark                         Time             CPU   Iterations
///     -------------------------------------------------------------------
///     BM_sched_getcpu_current        1.68 ns         1.68 ns    418951729
///     BM_sched_getcpu_rseq          0.279 ns        0.278 ns   1000000000
///     BM_sched_getcpu_vsyscall       2.51 ns         2.51 ns    277748021
///     BM_sched_getcpu_syscall        68.6 ns         68.4 ns     10001481
///


#include <benchmark/benchmark.h>

#if defined(OS_LINUX)

#include <cstddef>
#include <cstdint>
#include <dlfcn.h>
#include <sched.h>
#include <sys/syscall.h>
#include <unistd.h>

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wreserved-identifier"
extern "C"
{
    /// The rseq area location of the libc registration, exported by glibc >= 2.35.
    /// Weak so the binary links against libcs without them; the address is then null.
    extern const ptrdiff_t __rseq_offset __attribute__((weak));
    extern const unsigned int __rseq_size __attribute__((weak));
}
#pragma clang diagnostic pop

namespace
{

/// `cpu_id` lives at offset 4 in the kernel rseq ABI.
int32_t rseqCurrentCPU()
{
    const char * tp = static_cast<const char *>(__builtin_thread_pointer());
    return static_cast<int32_t>(*reinterpret_cast<const volatile uint32_t *>(tp + __rseq_offset + 4));
}

bool rseqUsable()
{
    /// The registered area must cover at least the `cpu_id` field (offset 4, size 4);
    /// `__rseq_size` is 0 when registration was disabled or failed.
    if (&__rseq_size == nullptr || __rseq_size < 8)
        return false;
    /// The kernel uses negative sentinels in `cpu_id`: -1 (UNINITIALIZED) and
    /// -2 (REGISTRATION_FAILED). The production `sched_getcpu`
    /// (base/glibc-compatibility/musl/sched_getcpu.c) rejects them before taking
    /// the rseq fast path; do the same so the benchmark measures the mechanism
    /// that is actually used, instead of timing a read of a sentinel value.
    return rseqCurrentCPU() >= 0;
}

void BM_sched_getcpu_current(benchmark::State & state)
{
    for (auto _ : state)
        benchmark::DoNotOptimize(sched_getcpu());
}
BENCHMARK(BM_sched_getcpu_current);

void BM_sched_getcpu_rseq(benchmark::State & state)
{
    if (!rseqUsable())
    {
        state.SkipWithError("rseq is not registered by libc or its cpu_id is not initialized");
        return;
    }
    for (auto _ : state)
        benchmark::DoNotOptimize(rseqCurrentCPU());
}
BENCHMARK(BM_sched_getcpu_rseq);

void BM_sched_getcpu_vsyscall(benchmark::State & state)
{
    using GetCPUFn = long (*)(unsigned *, unsigned *, void *); /// NOLINT
    GetCPUFn fn = nullptr;
    if (void * vdso = dlopen("linux-vdso.so.1", RTLD_LAZY | RTLD_NOLOAD))
        fn = reinterpret_cast<GetCPUFn>(dlsym(vdso, "__vdso_getcpu"));
    if (!fn)
    {
        state.SkipWithError("__vdso_getcpu is not exported by the vDSO");
        return;
    }
    unsigned cpu = 0;
    for (auto _ : state)
    {
        fn(&cpu, nullptr, nullptr);
        benchmark::DoNotOptimize(cpu);
    }
}
BENCHMARK(BM_sched_getcpu_vsyscall);

void BM_sched_getcpu_syscall(benchmark::State & state)
{
    unsigned cpu = 0;
    for (auto _ : state)
    {
        syscall(SYS_getcpu, &cpu, nullptr, nullptr);
        benchmark::DoNotOptimize(cpu);
    }
}
BENCHMARK(BM_sched_getcpu_syscall);

}

#endif

BENCHMARK_MAIN();
