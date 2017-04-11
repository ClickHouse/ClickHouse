#include <iostream>
#include <cstring>
#include <thread>
#include <Common/ArrayCache.h>
#include <IO/ReadHelpers.h>


template <typename Cache>
void printStats(const Cache & cache)
{
    typename Cache::Statistics statistics = cache.getStatistics();
    std::cerr
        << "total_chunks_size: " << statistics.total_chunks_size << "\n"
        << "total_allocated_size: " << statistics.total_allocated_size << "\n"
        << "total_size_currently_initialized: " << statistics.total_size_currently_initialized << "\n"
        << "total_size_in_use: " << statistics.total_size_in_use << "\n"
        << "num_chunks: " << statistics.num_chunks << "\n"
        << "num_regions: " << statistics.num_regions << "\n"
        << "num_free_regions: " << statistics.num_free_regions << "\n"
        << "num_regions_in_use: " << statistics.num_regions_in_use << "\n"
        << "num_keyed_regions: " << statistics.num_keyed_regions << "\n"
        << "hits: " << statistics.hits << "\n"
        << "concurrent_hits: " << statistics.concurrent_hits << "\n"
        << "misses: " << statistics.misses << "\n"
        << "allocations: " << statistics.allocations << "\n"
        << "allocated_bytes: " << statistics.allocated_bytes << "\n"
        << "evictions: " << statistics.evictions << "\n"
        << "evicted_bytes: " << statistics.evicted_bytes << "\n"
        << "secondary_evictions: " << statistics.secondary_evictions << "\n"
        << "\n";
}

/** Example:
  * time ./array_cache 68000000 1 10000000 2000000 200
  */


int main(int argc, char ** argv)
{
    size_t cache_size = DB::parse<size_t>(argv[1]);
    size_t num_threads = DB::parse<size_t>(argv[2]);
    size_t num_iterations = DB::parse<size_t>(argv[3]);
    size_t region_max_size = DB::parse<size_t>(argv[4]);
    size_t max_key = DB::parse<size_t>(argv[5]);

    using Cache = ArrayCache<int, int>;
    Cache cache(cache_size);

    std::vector<std::thread> threads;
    for (size_t i = 0; i < num_threads; ++i)
    {
        threads.emplace_back([&]
        {
            std::mt19937 generator(randomSeed());

            for (size_t i = 0; i < num_iterations; ++i)
            {
                size_t size = std::uniform_int_distribution<size_t>(1, region_max_size)(generator);
                int key = std::uniform_int_distribution<int>(1, max_key)(generator);

                cache.getOrSet(
                    key,
                    [=]{ return size; },
                    [=](void * ptr, int & payload)
                    {
                        payload = i;
                //        memset(ptr, i, size);
                    },
                    nullptr);

            //    printStats(cache);
            }
        });
    }

    std::atomic_bool stop{};

    std::thread stats_thread([&]
    {
        while (!stop)
        {
            usleep(100000);
            printStats(cache);
        }
    });

    for (auto & thread : threads)
        thread.join();

    stop = true;
    stats_thread.join();

    return 0;

/*
    using Cache = ArrayCache<int, int>;
    Cache cache(64 * 1024 * 1024);

    cache.getOrSet(
        1,
        [=]{ return 32 * 1024 * 1024; },
        [=](void * ptr, int & payload)
        {
            payload = 1;
        },
        nullptr);

    printStats(cache);

    cache.getOrSet(
        2,
        [=]{ return 32 * 1024 * 1024; },
        [=](void * ptr, int & payload)
        {
            payload = 2;
        },
        nullptr);

    printStats(cache);

    cache.getOrSet(
        3,
        [=]{ return 32 * 1024 * 1024; },
        [=](void * ptr, int & payload)
        {
            payload = 3;
        },
        nullptr);

    printStats(cache);*/
}
