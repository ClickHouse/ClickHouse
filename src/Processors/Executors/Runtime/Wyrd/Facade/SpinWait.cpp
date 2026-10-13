#include <Processors/Executors/Runtime/Wyrd/Facade/SpinWait.h>

#if defined(WYRD_SIMULATION)
#    include <Processors/Executors/Runtime/Wyrd/Strategies/Simulation/Syscall.h>
#elif defined(WYRD_ADVERSARY)
#    include <Processors/Executors/Runtime/Wyrd/Strategies/Adversary/InjectFault.h>
#endif

#include <algorithm>

namespace DB::Runtime::Wyrd
{

namespace
{

constexpr size_t max_round = 6;

void cpuPause()
{
#if defined(__x86_64__)
    __builtin_ia32_pause();
#elif defined(__aarch64__)
    asm volatile("yield" ::: "memory");
#endif
}

void pauseRounds(size_t rounds)
{
    const size_t pauses = size_t{1} << std::min(rounds, max_round);
    for (size_t i = 0; i < pauses; ++i)
        cpuPause();
}

}

#if defined(WYRD_SIMULATION)

void SpinWait::spin(std::source_location location)
{
    yield(location);
    pauseRounds(rounds);
    ++rounds;
}

#elif defined(WYRD_ADVERSARY)

void SpinWait::spin(std::source_location location)
{
    injectFault("spin_wait::spin", location);
    pauseRounds(rounds);
    ++rounds;
}

#else

void SpinWait::spin(std::source_location)
{
    pauseRounds(rounds);
    ++rounds;
}

#endif

}
