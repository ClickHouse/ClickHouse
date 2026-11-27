#include <Common/Wyrd/Facade/SpinWait.h>

#if defined(WYRD_ENABLED)
#    include <Common/Wyrd/Strategies/Simulation/Syscall.h>
#    include <Common/Wyrd/Strategies/Adversary/InjectFault.h>
#    include <Common/Wyrd/Strategies/Mode.h>
#endif

#include <algorithm>

namespace DB::Wyrd
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

#if defined(WYRD_ENABLED)

namespace SimulationMode
{

static void spin(size_t & rounds, std::source_location location)
{
    yield(location);
    pauseRounds(rounds);
    ++rounds;
}

}

namespace AdversaryMode
{

static void spin(size_t & rounds, std::source_location location)
{
    injectFault("spin_wait::spin", location);
    pauseRounds(rounds);
    ++rounds;
}

}

void SpinWait::spin(std::source_location location)
{
    if (getMode() == Mode::Simulation)
        SimulationMode::spin(rounds, location);
    else
        AdversaryMode::spin(rounds, location);
}

#else

void SpinWait::spin(std::source_location)
{
    pauseRounds(rounds);
    ++rounds;
}

#endif

}
