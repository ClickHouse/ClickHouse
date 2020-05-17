#include <iostream>
#include <cstring>
#include <thread>
#include <pcg_random.hpp>
#include <Common/Allocators/IGrabberAllocator.h>
#include <IO/ReadHelpers.h>

using namespace DB;

template <typename Cache>
void printStats(const Cache & cache)
{
    auto statistics = cache.getStats();

    std::cerr
        << "total_chunks_size: " << statistics.total_chunks_size << "\n"
        << "total_allocated_size: " << statistics.total_allocated_size << "\n"
        << "total_size_currently_initialized: " << statistics.total_currently_initialized_size << "\n"
        << "total_size_in_use: " << statistics.total_currently_used_size << "\n"
        << "num_chunks: " << statistics.chunks_count << "\n"
        << "num_regions: " << statistics.all_regions_count << "\n"
        << "num_free_regions: " << statistics.free_regions_count << "\n"
        << "num_regions_in_use: " << statistics.used_regions_count << "\n"
        << "num_keyed_regions: " << statistics.keyed_regions_count << "\n"
        << "hits: " << statistics.hits << "\n"
        << "concurrent_hits: " << statistics.concurrent_hits << "\n"
        << "misses: " << statistics.misses << "\n"
        << "allocations: " << statistics.allocations << "\n"
        << "allocated_bytes: " << statistics.allocated_bytes << "\n"
        << "evictions: " << statistics.evictions << "\n"
        << "evicted_bytes: " << statistics.evicted_bytes << "\n"
        << "secondary_evictions: " << statistics.secondary_evictions << "\n"
        << std::endl;
}

/**
 * Example:
 * time ./igrabber_allocator 68000000 1 10000000 2000000 200
 */

int main(int argc, char ** argv) noexcept
{
    if (argc < 6)
    {
        std::cerr << "Usage: program cache_size num_threads num_iterations region_max_size max_key\n";
        return 1;
    }

    size_t cache_size = DB::parse<size_t>(argv[1]);
    size_t num_threads = DB::parse<size_t>(argv[2]);
    size_t num_iterations = DB::parse<size_t>(argv[3]);
    size_t region_max_size = DB::parse<size_t>(argv[4]);
    size_t max_key = DB::parse<size_t>(argv[5]);

    IGrabberAllocator<int, int> cache(cache_size);

    std::vector<std::thread> threads;

    for (size_t i = 0; i < num_threads; ++i)
    {
        threads.emplace_back([&]
        {
            pcg64 generator(42);

            for (size_t j = 0; j < num_iterations; ++j)
            {
                size_t size = std::uniform_int_distribution<size_t>(1, region_max_size)(generator);
                int key = std::uniform_int_distribution<int>(1, max_key)(generator);

                auto && [ptr, produced] = cache.getOrSet(key, [=]{ return size; }, [=](void *) { return j; });

                if (!ptr && produced)
                {
                    std::cerr << "Cannot allocate memory" << std::endl;
                }
                else
                {
                    std::cerr << "Key: " << key << " Value: " << j
                              << " Size: " << size
                              << " Miss: " << produced
                              << "Got value: " << (ptr ? *ptr : -1) << std::endl;
                }
                //printStats(cache);
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

///    struct myData {
///        void * data_ptr; // Holds a size_t
///    };
///
///    IGrabberAllocator<UInt128, myData> cache2(64 * 1024 * 1024);
///
///    const auto init = [=](void * ptr)
///        {
///            memset(ptr, 99999, sizeof(size_t));
///            return myData{ptr};
///        };
///
///    cache2.getOrSet(UInt128{1}, [=]{ return 32 * 1024 * 1024; }, init);
///
///    printStats(cache2);
///
///    cache2.getOrSet(UInt128{2}, [=]{ return 32 * 1024 * 1024; }, init);
///
///    printStats(cache2);
///
///    cache2.getOrSet(UInt128{3}, [=]{ return 32 * 1024 * 1024; }, init);
///
///    printStats(cache2);
}

