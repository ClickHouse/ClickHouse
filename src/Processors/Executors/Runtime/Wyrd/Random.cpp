#include <Processors/Executors/Runtime/Wyrd/Random.h>

#include <Common/randomSeed.h>

#include <pcg_random.hpp>

#include <atomic>
#include <utility>

namespace DB::Runtime::Wyrd
{

namespace
{

const UInt64 base_seed = randomSeed();
std::atomic<UInt64> thread_counter{0};

pcg64 & generator()
{
    thread_local pcg64 rng(base_seed, thread_counter.fetch_add(1));
    return rng;
}

}

void setRandomSeed(UInt64 seed)
{
    generator().seed(seed, 0);
}

UInt64 randomNumber()
{
    return generator()();
}

size_t randomBetween(size_t from, size_t to)
{
    return from + randomNumber() % (to - from + 1);
}

bool randomEventEvery(size_t freq)
{
    if (freq == 0)
        return false;

    return generator()() % freq == 0;
}

template <typename T>
T popRandom(std::vector<T> & items)
{
    size_t index = randomNumber() % items.size();
    T item = std::move(items[index]);
    items[index] = std::move(items.back());
    items.pop_back();
    return item;
}

struct SimulatedThread;
template SimulatedThread * popRandom(std::vector<SimulatedThread *> & items);

}
