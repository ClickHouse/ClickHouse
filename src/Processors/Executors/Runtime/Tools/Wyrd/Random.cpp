#include <Processors/Executors/Runtime/Tools/Wyrd/Random.h>

#include <Common/randomSeed.h>

#include <pcg_random.hpp>

#include <atomic>
#include <limits>
#include <utility>

namespace DB::Runtime::Wyrd
{

namespace
{

std::atomic<UInt64> base_seed{randomSeed()};
std::atomic<UInt64> epoch{0};
std::atomic<UInt64> thread_counter{0};

struct Generator
{
    pcg64 rng;
    UInt64 epoch_seen = std::numeric_limits<UInt64>::max();
};

pcg64 & generator()
{
    thread_local Generator generator;

    UInt64 current_epoch = epoch.load();
    if (generator.epoch_seen != current_epoch)
    {
        generator.rng.seed(base_seed.load(), thread_counter.fetch_add(1));
        generator.epoch_seen = current_epoch;
    }
    return generator.rng;
}

}

UInt64 randomNumber()
{
    return generator()();
}

bool randomEventEvery(size_t freq)
{
    return generator()() % freq == 0;
}

void setRandomSeed(UInt64 seed)
{
    base_seed.store(seed);
    thread_counter.store(0);
    epoch.fetch_add(1);
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
