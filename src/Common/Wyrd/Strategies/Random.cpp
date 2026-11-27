#include <Common/Wyrd/Strategies/Random.h>

#include <Common/randomSeed.h>

#include <pcg_random.hpp>

#include <atomic>
#include <random>

namespace DB::Wyrd
{

namespace
{

std::atomic<UInt64> thread_counter{0};

pcg64 & generator()
{
    static const UInt64 base_seed = randomSeed();
    thread_local pcg64 rng(base_seed, thread_counter.fetch_add(1));
    return rng;
}

}

void setRandomSeed(UInt64 seed)
{
    generator().seed(seed, 0);
}

UInt64 randomInt(UInt64 from, UInt64 to)
{
    return std::uniform_int_distribution<UInt64>(from, to)(generator());
}

double randomDouble(double from, double to)
{
    return std::uniform_real_distribution<double>(from, to)(generator());
}

bool randomBool(double probability)
{
    return randomDouble(0.0, 1.0) < probability;
}

}
