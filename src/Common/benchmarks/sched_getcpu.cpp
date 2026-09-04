/// Cost of obtaining the current CPU id by different mechanisms:
///   - `sched_getcpu`: whatever the libc in this build provides (rseq TLS read, vDSO, or syscall)
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
///     BM_sched_getcpu_vsyscall       2.51 ns         2.51 ns    277748021
///     BM_sched_getcpu_syscall        68.6 ns         68.4 ns     10001481
///


#include <benchmark/benchmark.h>

#if defined(OS_LINUX)

#include <cstddef>
#include <dlfcn.h>
#include <sched.h>
#include <sys/syscall.h>
#include <unistd.h>

namespace
{

void BM_sched_getcpu_current(benchmark::State & state)
{
    for (auto _ [[maybe_unused]] : state)
        benchmark::DoNotOptimize(sched_getcpu());
}
BENCHMARK(BM_sched_getcpu_current);

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
    for (auto _ [[maybe_unused]] : state)
    {
        fn(&cpu, nullptr, nullptr);
        benchmark::DoNotOptimize(cpu);
    }
}
BENCHMARK(BM_sched_getcpu_vsyscall);

void BM_sched_getcpu_syscall(benchmark::State & state)
{
    unsigned cpu = 0;
    for (auto _ [[maybe_unused]] : state)
    {
        syscall(SYS_getcpu, &cpu, nullptr, nullptr);
        benchmark::DoNotOptimize(cpu);
    }
}
BENCHMARK(BM_sched_getcpu_syscall);

}

#endif

BENCHMARK_MAIN();
