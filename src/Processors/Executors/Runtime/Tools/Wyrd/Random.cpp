#include <Processors/Executors/Runtime/Tools/Wyrd/Random.h>
#include <Common/randomSeed.h>
#include <base/defines.h>

#include <pcg_random.hpp>

#include <atomic>

namespace DB::Runtime::Wyrd
{

namespace
{

std::atomic<UInt64> base_seed{randomSeed()};
std::atomic<UInt64> thread_counter{0};

pcg64 & generator()
{
    thread_local pcg64 rng(base_seed.load(), thread_counter.fetch_add(1));
    return rng;
}

}

UInt64 randomNumber()
{
    return generator()();
}

bool randomEventEvery(size_t freq)
{
    chassert(freq > 0);
    return generator()() % freq == 0;
}

void setRandomSeed(UInt64 seed)
{
    base_seed.store(seed);
    thread_counter.store(0);
}

}
