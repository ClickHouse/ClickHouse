#include <iostream>
#include <iomanip>
#include <mutex>
#include <atomic>

//#define DBMS_HASH_MAP_DEBUG_RESIZES

#include <Interpreters/AggregationCommon.h>

#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/TwoLevelHashMap.h>
//#include <Common/HashTable/HashTableWithSmallLocks.h>
//#include <Common/HashTable/HashTableMerge.h>

#include <IO/ReadBufferFromFile.h>
#include <Compression/CompressedReadBuffer.h>

#include <Common/Stopwatch.h>
#include <Common/ThreadPool.h>


using Key = UInt64;
using Value = UInt64;

using Source = std::vector<Key>;

using Map = HashMap<Key, Value>;
using MapTwoLevel = TwoLevelHashMap<Key, Value>;


struct SmallLock
{
    std::atomic<int> locked {false};

    bool try_lock()
    {
        int expected = 0;
        return locked.compare_exchange_strong(expected, 1, std::memory_order_acquire);
    }

    void unlock()
    {
        locked.store(0, std::memory_order_release);
    }
};

struct __attribute__((__aligned__(64))) AlignedSmallLock : public SmallLock
{
    char dummy[64 - sizeof(SmallLock)];
};


using Mutex = std::mutex;


/*using MapSmallLocks = HashTableWithSmallLocks<
    Key,
    HashTableCellWithLock<
        Key,
        HashMapCell<Key, Value, DefaultHash<Key>> >,
    DefaultHash<Key>,
    HashTableGrower<21>,
    HashTableAllocator>;*/


void aggregate1(Map & map, Source::const_iterator begin, Source::const_iterator end)
{
    for (auto it = begin; it != end; ++it)
        ++map[*it];
}

#if !__clang__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#endif

void aggregate12(Map & map, Source::const_iterator begin, Source::const_iterator end)
{
    Map::iterator found;
    auto prev_it = end;
    for (auto it = begin; it != end; ++it)
    {
        if (*it == *prev_it)
        {
            ++found->getSecond();
            continue;
        }
        prev_it = it;

        bool inserted;
        map.emplace(*it, found, inserted);
        ++found->getSecond();
    }
}

void aggregate2(MapTwoLevel & map, Source::const_iterator begin, Source::const_iterator end)
{
    for (auto it = begin; it != end; ++it)
        ++map[*it];
}

void aggregate22(MapTwoLevel & map, Source::const_iterator begin, Source::const_iterator end)
{
    MapTwoLevel::iterator found;
    auto prev_it = end;
    for (auto it = begin; it != end; ++it)
    {
        if (*it == *prev_it)
        {
            ++found->getSecond();
            continue;
        }
        prev_it = it;

        bool inserted;
        map.emplace(*it, found, inserted);
        ++found->getSecond();
    }
}

#if !__clang__
#pragma GCC diagnostic pop
#endif

void merge2(MapTwoLevel * maps, size_t num_threads, size_t bucket)
{
    for (size_t i = 1; i < num_threads; ++i)
        for (auto it = maps[i].impls[bucket].begin(); it != maps[i].impls[bucket].end(); ++it)
            maps[0].impls[bucket][it->getFirst()] += it->getSecond();
}

void aggregate3(Map & local_map, Map & global_map, Mutex & mutex, Source::const_iterator begin, Source::const_iterator end)
{
    static constexpr size_t threshold = 65536;

    for (auto it = begin; it != end; ++it)
    {
        Map::iterator found = local_map.find(*it);

        if (found != local_map.end())
            ++found->getSecond();
        else if (local_map.size() < threshold)
            ++local_map[*it];    /// TODO You could do one lookup, not two.
        else
        {
            if (mutex.try_lock())
            {
                ++global_map[*it];
                mutex.unlock();
            }
            else
                ++local_map[*it];
        }
    }
}

void aggregate33(Map & local_map, Map & global_map, Mutex & mutex, Source::const_iterator begin, Source::const_iterator end)
{
    static constexpr size_t threshold = 65536;

    for (auto it = begin; it != end; ++it)
    {
        Map::iterator found;
        bool inserted;
        local_map.emplace(*it, found, inserted);
        ++found->getSecond();

        if (inserted && local_map.size() == threshold)
        {
            std::lock_guard<Mutex> lock(mutex);
            for (auto & value_type : local_map)
                global_map[value_type.getFirst()] += value_type.getSecond();

            local_map.clear();
        }
    }
}

void aggregate4(Map & local_map, MapTwoLevel & global_map, Mutex * mutexes, Source::const_iterator begin, Source::const_iterator end)
{
    static constexpr size_t threshold = 65536;
    static constexpr size_t block_size = 8192;

    auto it = begin;
    while (it != end)
    {
        auto block_end = std::min(end, it + block_size);

        if (local_map.size() < threshold)
        {
            for (; it != block_end; ++it)
                ++local_map[*it];
        }
        else
        {
            for (; it != block_end; ++it)
            {
                Map::iterator found = local_map.find(*it);

                if (found != local_map.end())
                    ++found->getSecond();
                else
                {
                    size_t hash_value = global_map.hash(*it);
                    size_t bucket = global_map.getBucketFromHash(hash_value);

                    if (mutexes[bucket].try_lock())
                    {
                        ++global_map.impls[bucket][*it];
                        mutexes[bucket].unlock();
                    }
                    else
                        ++local_map[*it];
                }
            }
        }
    }
}
/*
void aggregate5(Map & local_map, MapSmallLocks & global_map, Source::const_iterator begin, Source::const_iterator end)
{
    static constexpr size_t threshold = 65536;

    for (auto it = begin; it != end; ++it)
    {
        Map::iterator found = local_map.find(*it);

        if (found != local_map.end())
            ++found->second;
        else if (local_map.size() < threshold)
            ++local_map[*it];    /// TODO You could do one lookup, not two.
        else
        {
            SmallScopedLock lock;
            MapSmallLocks::iterator insert_it;
            bool inserted;

            if (global_map.tryEmplace(*it, insert_it, inserted, lock))
                ++insert_it->second;
            else
                ++local_map[*it];
        }
    }
}*/



int main(int argc, char ** argv)
{
    size_t n = atoi(argv[1]);
    size_t num_threads = atoi(argv[2]);
    size_t method = argc <= 3 ? 0 : atoi(argv[3]);

    std::cerr << std::fixed << std::setprecision(2);

    ThreadPool pool(num_threads);

    Source data(n);

    {
        Stopwatch watch;
        DB::ReadBufferFromFileDescriptor in1(STDIN_FILENO);
        DB::CompressedReadBuffer in2(in1);

        in2.readStrict(reinterpret_cast<char*>(data.data()), sizeof(data[0]) * n);

        watch.stop();
        std::cerr << std::fixed << std::setprecision(2)
            << "Vector. Size: " << n
            << ", elapsed: " << watch.elapsedSeconds()
            << " (" << n / watch.elapsedSeconds() << " elem/sec.)"
            << std::endl << std::endl;
    }

    if (!method || method == 1)
    {
        /** Option 1.
          * In different threads, we aggregate independently into different hash tables.
          * Then merge them together.
          */

        std::vector<Map> maps(num_threads);

        Stopwatch watch;

        for (size_t i = 0; i < num_threads; ++i)
            pool.schedule(std::bind(aggregate1,
                std::ref(maps[i]),
                data.begin() + (data.size() * i) / num_threads,
                data.begin() + (data.size() * (i + 1)) / num_threads));

        pool.wait();

        watch.stop();
        double time_aggregated = watch.elapsedSeconds();
        std::cerr
            << "Aggregated in " << time_aggregated
            << " (" << n / time_aggregated << " elem/sec.)"
            << std::endl;

        size_t size_before_merge = 0;
        std::cerr << "Sizes: ";
        for (size_t i = 0; i < num_threads; ++i)
        {
            std::cerr << (i == 0 ? "" : ", ") << maps[i].size();
            size_before_merge += maps[i].size();
        }
        std::cerr << std::endl;

        watch.restart();

        for (size_t i = 1; i < num_threads; ++i)
            for (auto it = maps[i].begin(); it != maps[i].end(); ++it)
                maps[0][it->getFirst()] += it->getSecond();

        watch.stop();
        double time_merged = watch.elapsedSeconds();
        std::cerr
            << "Merged in " << time_merged
            << " (" << size_before_merge / time_merged << " elem/sec.)"
            << std::endl;

        double time_total = time_aggregated + time_merged;
        std::cerr
            << "Total in " << time_total
            << " (" << n / time_total << " elem/sec.)"
            << std::endl;
        std::cerr << "Size: " << maps[0].size() << std::endl << std::endl;
    }

    if (!method || method == 12)
    {
        /** The same, but with optimization for consecutive identical values.
          */

        std::vector<Map> maps(num_threads);

        Stopwatch watch;

        for (size_t i = 0; i < num_threads; ++i)
            pool.schedule(std::bind(aggregate12,
                                    std::ref(maps[i]),
                                    data.begin() + (data.size() * i) / num_threads,
                                    data.begin() + (data.size() * (i + 1)) / num_threads));

        pool.wait();

        watch.stop();
        double time_aggregated = watch.elapsedSeconds();
        std::cerr
        << "Aggregated in " << time_aggregated
        << " (" << n / time_aggregated << " elem/sec.)"
        << std::endl;

        size_t size_before_merge = 0;
        std::cerr << "Sizes: ";
        for (size_t i = 0; i < num_threads; ++i)
        {
            std::cerr << (i == 0 ? "" : ", ") << maps[i].size();
            size_before_merge += maps[i].size();
        }
        std::cerr << std::endl;

        watch.restart();

        for (size_t i = 1; i < num_threads; ++i)
            for (auto it = maps[i].begin(); it != maps[i].end(); ++it)
                maps[0][it->getFirst()] += it->getSecond();

        watch.stop();

        double time_merged = watch.elapsedSeconds();
        std::cerr
        << "Merged in " << time_merged
        << " (" << size_before_merge / time_merged << " elem/sec.)"
        << std::endl;

        double time_total = time_aggregated + time_merged;
        std::cerr
        << "Total in " << time_total
        << " (" << n / time_total << " elem/sec.)"
        << std::endl;
        std::cerr << "Size: " << maps[0].size() << std::endl << std::endl;
    }

    if (!method || method == 11)
    {
        /** Option 11.
          * Same as option 1, but with merge, the order of the cycles is changed,
          *  which potentially can give better cache locality.
          *
          * In practice, there is no difference.
          */

        std::vector<Map> maps(num_threads);

        Stopwatch watch;

        for (size_t i = 0; i < num_threads; ++i)
            pool.schedule(std::bind(aggregate1,
                std::ref(maps[i]),
                data.begin() + (data.size() * i) / num_threads,
                data.begin() + (data.size() * (i + 1)) / num_threads));

        pool.wait();

        watch.stop();
        double time_aggregated = watch.elapsedSeconds();
        std::cerr
        << "Aggregated in " << time_aggregated
        << " (" << n / time_aggregated << " elem/sec.)"
        << std::endl;

        size_t size_before_merge = 0;
        std::cerr << "Sizes: ";
        for (size_t i = 0; i < num_threads; ++i)
        {
            std::cerr << (i == 0 ? "" : ", ") << maps[i].size();
            size_before_merge += maps[i].size();
        }
        std::cerr << std::endl;

        watch.restart();

        std::vector<Map::iterator> iterators(num_threads);
        for (size_t i = 1; i < num_threads; ++i)
            iterators[i] = maps[i].begin();

        while (true)
        {
            bool finish = true;
            for (size_t i = 1; i < num_threads; ++i)
            {
                if (iterators[i] == maps[i].end())
                    continue;

                finish = false;
                maps[0][iterators[i]->getFirst()] += iterators[i]->getSecond();
                ++iterators[i];
            }

            if (finish)
                break;
        }

        watch.stop();
        double time_merged = watch.elapsedSeconds();
        std::cerr
        << "Merged in " << time_merged
        << " (" << size_before_merge / time_merged << " elem/sec.)"
        << std::endl;

        double time_total = time_aggregated + time_merged;
        std::cerr
        << "Total in " << time_total
        << " (" << n / time_total << " elem/sec.)"
        << std::endl;
        std::cerr << "Size: " << maps[0].size() << std::endl << std::endl;
    }

    if (!method || method == 2)
    {
        /** Option 2.
          * In different threads, we aggregate independently into different two-level hash tables.
          * Then merge them together, parallelizing by the first level buckets.
          * When using hash tables of large sizes (10 million elements or more),
          *  and a large number of threads (8-32), the merge is a bottleneck,
          *  and has a performance advantage of 4 times.
          */

        std::vector<MapTwoLevel> maps(num_threads);

        Stopwatch watch;

        for (size_t i = 0; i < num_threads; ++i)
            pool.schedule(std::bind(aggregate2,
                std::ref(maps[i]),
                data.begin() + (data.size() * i) / num_threads,
                data.begin() + (data.size() * (i + 1)) / num_threads));

        pool.wait();

        watch.stop();
        double time_aggregated = watch.elapsedSeconds();
        std::cerr
            << "Aggregated in " << time_aggregated
            << " (" << n / time_aggregated << " elem/sec.)"
            << std::endl;

        size_t size_before_merge = 0;
        std::cerr << "Sizes: ";
        for (size_t i = 0; i < num_threads; ++i)
        {
            std::cerr << (i == 0 ? "" : ", ") << maps[i].size();
            size_before_merge += maps[i].size();
        }
        std::cerr << std::endl;

        watch.restart();

        for (size_t i = 0; i < MapTwoLevel::NUM_BUCKETS; ++i)
            pool.schedule(std::bind(merge2,
                maps.data(), num_threads, i));

        pool.wait();

        watch.stop();
        double time_merged = watch.elapsedSeconds();
        std::cerr
            << "Merged in " << time_merged
            << " (" << size_before_merge / time_merged << " elem/sec.)"
            << std::endl;

        double time_total = time_aggregated + time_merged;
        std::cerr
            << "Total in " << time_total
            << " (" << n / time_total << " elem/sec.)"
            << std::endl;

        std::cerr << "Size: " << maps[0].size() << std::endl << std::endl;
    }

    if (!method || method == 22)
    {
        std::vector<MapTwoLevel> maps(num_threads);

        Stopwatch watch;

        for (size_t i = 0; i < num_threads; ++i)
            pool.schedule(std::bind(aggregate22,
                                    std::ref(maps[i]),
                                    data.begin() + (data.size() * i) / num_threads,
                                    data.begin() + (data.size() * (i + 1)) / num_threads));

        pool.wait();

        watch.stop();
        double time_aggregated = watch.elapsedSeconds();
        std::cerr
        << "Aggregated in " << time_aggregated
        << " (" << n / time_aggregated << " elem/sec.)"
        << std::endl;

        size_t size_before_merge = 0;
        std::cerr << "Sizes: ";
        for (size_t i = 0; i < num_threads; ++i)
        {
            std::cerr << (i == 0 ? "" : ", ") << maps[i].size();
            size_before_merge += maps[i].size();
        }
        std::cerr << std::endl;

        watch.restart();

        for (size_t i = 0; i < MapTwoLevel::NUM_BUCKETS; ++i)
            pool.schedule(std::bind(merge2, maps.data(), num_threads, i));

        pool.wait();

        watch.stop();
        double time_merged = watch.elapsedSeconds();
        std::cerr
        << "Merged in " << time_merged
        << " (" << size_before_merge / time_merged << " elem/sec.)"
        << std::endl;

        double time_total = time_aggregated + time_merged;
        std::cerr
        << "Total in " << time_total
        << " (" << n / time_total << " elem/sec.)"
        << std::endl;

        std::cerr << "Size: " << maps[0].size() << std::endl << std::endl;
    }

    if (!method || method == 3)
    {
        /** Option 3.
          * In different threads, we aggregate independently into different hash tables,
          *  until their size becomes large enough.
          * If the size of the local hash table is large, and there is no element in it,
          *  then we insert it into one global hash table, protected by mutex,
          *  and if mutex failed to capture, then insert it into the local one.
          * Then merge all the local hash tables to the global one.
          * This method is bad - a lot of contention.
          */

        std::vector<Map> local_maps(num_threads);
        Map global_map;
        Mutex mutex;

        Stopwatch watch;

        for (size_t i = 0; i < num_threads; ++i)
            pool.schedule(std::bind(aggregate3,
                std::ref(local_maps[i]),
                std::ref(global_map),
                std::ref(mutex),
                data.begin() + (data.size() * i) / num_threads,
                data.begin() + (data.size() * (i + 1)) / num_threads));

        pool.wait();

        watch.stop();
        double time_aggregated = watch.elapsedSeconds();
        std::cerr
            << "Aggregated in " << time_aggregated
            << " (" << n / time_aggregated << " elem/sec.)"
            << std::endl;

        size_t size_before_merge = 0;
        std::cerr << "Sizes (local): ";
        for (size_t i = 0; i < num_threads; ++i)
        {
            std::cerr << (i == 0 ? "" : ", ") << local_maps[i].size();
            size_before_merge += local_maps[i].size();
        }
        std::cerr << std::endl;
        std::cerr << "Size (global): " << global_map.size() << std::endl;
        size_before_merge += global_map.size();

        watch.restart();

        for (size_t i = 0; i < num_threads; ++i)
            for (auto it = local_maps[i].begin(); it != local_maps[i].end(); ++it)
                global_map[it->getFirst()] += it->getSecond();

        pool.wait();

        watch.stop();
        double time_merged = watch.elapsedSeconds();
        std::cerr
            << "Merged in " << time_merged
            << " (" << size_before_merge / time_merged << " elem/sec.)"
            << std::endl;

        double time_total = time_aggregated + time_merged;
        std::cerr
            << "Total in " << time_total
            << " (" << n / time_total << " elem/sec.)"
            << std::endl;

        std::cerr << "Size: " << global_map.size() << std::endl << std::endl;
    }

    if (!method || method == 33)
    {
        /** Option 33.
         * In different threads, we aggregate independently into different hash tables,
         *  until their size becomes large enough.
         * Then we insert the data to the global hash table, protected by mutex, and continue.
         */

        std::vector<Map> local_maps(num_threads);
        Map global_map;
        Mutex mutex;

        Stopwatch watch;

        for (size_t i = 0; i < num_threads; ++i)
            pool.schedule(std::bind(aggregate33,
                std::ref(local_maps[i]),
                std::ref(global_map),
                std::ref(mutex),
                data.begin() + (data.size() * i) / num_threads,
                data.begin() + (data.size() * (i + 1)) / num_threads));

        pool.wait();

        watch.stop();
        double time_aggregated = watch.elapsedSeconds();
        std::cerr
        << "Aggregated in " << time_aggregated
        << " (" << n / time_aggregated << " elem/sec.)"
        << std::endl;

        size_t size_before_merge = 0;
        std::cerr << "Sizes (local): ";
        for (size_t i = 0; i < num_threads; ++i)
        {
            std::cerr << (i == 0 ? "" : ", ") << local_maps[i].size();
            size_before_merge += local_maps[i].size();
        }
        std::cerr << std::endl;
        std::cerr << "Size (global): " << global_map.size() << std::endl;
        size_before_merge += global_map.size();

        watch.restart();

        for (size_t i = 0; i < num_threads; ++i)
            for (auto it = local_maps[i].begin(); it != local_maps[i].end(); ++it)
                global_map[it->getFirst()] += it->getSecond();

        pool.wait();

        watch.stop();
        double time_merged = watch.elapsedSeconds();
        std::cerr
        << "Merged in " << time_merged
        << " (" << size_before_merge / time_merged << " elem/sec.)"
        << std::endl;

        double time_total = time_aggregated + time_merged;
        std::cerr
        << "Total in " << time_total
        << " (" << n / time_total << " elem/sec.)"
        << std::endl;

        std::cerr << "Size: " << global_map.size() << std::endl << std::endl;
    }

    if (!method || method == 4)
    {
        /** Option 4.
          * In different threads, we aggregate independently into different hash tables,
          *  until their size becomes large enough.
          * If the size of the local hash table is large, and there is no element in it,
          *  then insert it into one of 256 global hash tables, each of which is under its mutex.
          * Then merge all local hash tables into the global one.
          * This method is not so bad with a lot of threads, but worse than the second one.
          */

        std::vector<Map> local_maps(num_threads);
        MapTwoLevel global_map;
        std::vector<Mutex> mutexes(MapTwoLevel::NUM_BUCKETS);

        Stopwatch watch;

        for (size_t i = 0; i < num_threads; ++i)
            pool.schedule(std::bind(aggregate4,
                std::ref(local_maps[i]),
                std::ref(global_map),
                mutexes.data(),
                data.begin() + (data.size() * i) / num_threads,
                data.begin() + (data.size() * (i + 1)) / num_threads));

        pool.wait();

        watch.stop();
        double time_aggregated = watch.elapsedSeconds();
        std::cerr
            << "Aggregated in " << time_aggregated
            << " (" << n / time_aggregated << " elem/sec.)"
            << std::endl;

        size_t size_before_merge = 0;
        std::cerr << "Sizes (local): ";
        for (size_t i = 0; i < num_threads; ++i)
        {
            std::cerr << (i == 0 ? "" : ", ") << local_maps[i].size();
            size_before_merge += local_maps[i].size();
        }
        std::cerr << std::endl;

        size_t sum_size = global_map.size();
        std::cerr << "Size (global): " << sum_size << std::endl;
        size_before_merge += sum_size;

        watch.restart();

        for (size_t i = 0; i < num_threads; ++i)
            for (auto it = local_maps[i].begin(); it != local_maps[i].end(); ++it)
                global_map[it->getFirst()] += it->getSecond();

        pool.wait();

        watch.stop();
        double time_merged = watch.elapsedSeconds();
        std::cerr
            << "Merged in " << time_merged
            << " (" << size_before_merge / time_merged << " elem/sec.)"
            << std::endl;

        double time_total = time_aggregated + time_merged;
        std::cerr
            << "Total in " << time_total
            << " (" << n / time_total << " elem/sec.)"
            << std::endl;

        std::cerr << "Size: " << global_map.size() << std::endl << std::endl;
    }

/*    if (!method || method == 5)
    {
    */  /** Option 5.
          * In different threads, we aggregate independently into different hash tables,
          *  until their size becomes large enough.
          * If the size of the local hash table is large and there is no element in it,
          *  then insert it into one global hash table containing small latches in each cell,
          *  and if the latch can not be captured, then insert it into the local one.
          * Then merge all local hash tables into the global one.
          */
/*
        Map local_maps[num_threads];
        MapSmallLocks global_map;

        Stopwatch watch;

        for (size_t i = 0; i < num_threads; ++i)
            pool.schedule(std::bind(aggregate5,
                std::ref(local_maps[i]),
                std::ref(global_map),
                data.begin() + (data.size() * i) / num_threads,
                data.begin() + (data.size() * (i + 1)) / num_threads));

        pool.wait();

        watch.stop();
        double time_aggregated = watch.elapsedSeconds();
        std::cerr
            << "Aggregated in " << time_aggregated
            << " (" << n / time_aggregated << " elem/sec.)"
            << std::endl;

        size_t size_before_merge = 0;
        std::cerr << "Sizes (local): ";
        for (size_t i = 0; i < num_threads; ++i)
        {
            std::cerr << (i == 0 ? "" : ", ") << local_maps[i].size();
            size_before_merge += local_maps[i].size();
        }
        std::cerr << std::endl;
        std::cerr << "Size (global): " << global_map.size() << std::endl;
        size_before_merge += global_map.size();

        watch.restart();

        for (size_t i = 0; i < num_threads; ++i)
            for (auto it = local_maps[i].begin(); it != local_maps[i].end(); ++it)
                global_map.insert(std::make_pair(it->first, 0)).first->second += it->second;

        pool.wait();

        watch.stop();
        double time_merged = watch.elapsedSeconds();
        std::cerr
            << "Merged in " << time_merged
            << " (" << size_before_merge / time_merged << " elem/sec.)"
            << std::endl;

        double time_total = time_aggregated + time_merged;
        std::cerr
            << "Total in " << time_total
            << " (" << n / time_total << " elem/sec.)"
            << std::endl;

        std::cerr << "Size: " << global_map.size() << std::endl << std::endl;
    }*/

    /*if (!method || method == 6)
    {
        *//** Option 6.
          * In different threads, we aggregate independently into different hash tables.
          * Then "merge" them, passing them in the same order of the keys.
          * Quite a slow option.
          */
/*
        std::vector<Map> maps(num_threads);

        Stopwatch watch;

        for (size_t i = 0; i < num_threads; ++i)
            pool.schedule(std::bind(aggregate1,
                std::ref(maps[i]),
                data.begin() + (data.size() * i) / num_threads,
                data.begin() + (data.size() * (i + 1)) / num_threads));

        pool.wait();

        watch.stop();
        double time_aggregated = watch.elapsedSeconds();
        std::cerr
            << "Aggregated in " << time_aggregated
            << " (" << n / time_aggregated << " elem/sec.)"
            << std::endl;

        size_t size_before_merge = 0;
        std::cerr << "Sizes: ";
        for (size_t i = 0; i < num_threads; ++i)
        {
            std::cerr << (i == 0 ? "" : ", ") << maps[i].size();
            size_before_merge += maps[i].size();
        }
        std::cerr << std::endl;

        watch.restart();

        using Maps = std::vector<Map *>;
        Maps maps_to_merge(num_threads);
        for (size_t i = 0; i < num_threads; ++i)
            maps_to_merge[i] = &maps[i];

        size_t size = 0;

        for (size_t i = 0; i < 100; ++i)
        processMergedHashTables(maps_to_merge,
            [] (Map::value_type & dst, const Map::value_type & src) { dst.second += src.second; },
            [&] (const Map::value_type & dst) { ++size; });

        watch.stop();
        double time_merged = watch.elapsedSeconds();
        std::cerr
            << "Merged in " << time_merged
            << " (" << size_before_merge / time_merged << " elem/sec.)"
            << std::endl;

        double time_total = time_aggregated + time_merged;
        std::cerr
            << "Total in " << time_total
            << " (" << n / time_total << " elem/sec.)"
            << std::endl;
        std::cerr << "Size: " << size << std::endl << std::endl;
    }*/

    return 0;
}
