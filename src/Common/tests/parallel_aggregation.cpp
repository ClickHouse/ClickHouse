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
using MapTwoLevel512 = TwoLevelHashMap<Key, Value, DefaultHash<Key>, TwoLevelHashTableGrower<>, HashTableAllocator, HashMapTable, 9>;
using MapTwoLevel128 = TwoLevelHashMap<Key, Value, DefaultHash<Key>, TwoLevelHashTableGrower<>, HashTableAllocator, HashMapTable, 7>;
using MapTwoLevel1024 = TwoLevelHashMap<Key, Value, DefaultHash<Key>, TwoLevelHashTableGrower<>, HashTableAllocator, HashMapTable, 10>;
using MapTwoLevel2048 = TwoLevelHashMap<Key, Value, DefaultHash<Key>, TwoLevelHashTableGrower<>, HashTableAllocator, HashMapTable, 11>;
using MapTwoLevel4096 = TwoLevelHashMap<Key, Value, DefaultHash<Key>, TwoLevelHashTableGrower<>, HashTableAllocator, HashMapTable, 12>;
using MapTwoLevel8192 = TwoLevelHashMap<Key, Value, DefaultHash<Key>, TwoLevelHashTableGrower<>, HashTableAllocator, HashMapTable, 13>;
using MapTwoLevel16384 = TwoLevelHashMap<Key, Value, DefaultHash<Key>, TwoLevelHashTableGrower<>, HashTableAllocator, HashMapTable, 14>;

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

struct SmallLockExt
{
    std::atomic<int> locked = 0;

    bool is_ready()
    {
        return locked == 1;
    }

    void set_ready()
    {
        locked = 1;
    }

    void set_done()
    {
        locked = 0;
    }
};

struct __attribute__((__aligned__(64))) AlignedSmallLock : public SmallLock
{
    char dummy[64 - sizeof(SmallLock)];
};

struct __attribute__((__aligned__(64))) AlignedSmallLockExt : public SmallLockExt
{
    char dummy[64 - sizeof(SmallLockExt)];
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

static size_t getThreadNum(size_t hash, __uint128_t num_threads)
{
    return static_cast<size_t>((static_cast<__uint128_t>(hash) * num_threads) >> 64);
}

static void aggregate1(Map & map, Source::const_iterator begin, Source::const_iterator end)
{
    for (auto it = begin; it != end; ++it)
        ++map[*it];
}

#if !__clang__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#endif

static void aggregate12(Map & map, Source::const_iterator begin, Source::const_iterator end)
{
    Map::LookupResult found = nullptr;
    auto prev_it = end;
    for (auto it = begin; it != end; ++it)
    {
        if (prev_it != end && *it == *prev_it)
        {
            assert(found != nullptr);
            ++found->getMapped();
            continue;
        }
        prev_it = it;

        bool inserted;
        map.emplace(*it, found, inserted);
        assert(found != nullptr);
        ++found->getMapped();
    }
}

static void aggregate15(Map & map, Source::const_iterator begin,Source::const_iterator end,
                        size_t min_value, size_t max_value)
{
    Map::LookupResult found = nullptr;
    auto prev_it = end;
    for (auto it = begin; it != end; ++it)
    {
        size_t hash = map.hash(*it);
        if (hash < min_value || hash > max_value)
            continue;
        if (prev_it != end && *it == *prev_it)
        {
            assert(found != nullptr);
            ++found->getMapped();
            continue;
        }
        prev_it = it;

        bool inserted;
        map.emplace(*it, found, inserted, hash);
        assert(found != nullptr);
        ++found->getMapped();
    }
}

static void aggregate151(Map & map, Source::const_iterator begin,Source::const_iterator end,
                         size_t num_threads, size_t bucket)
{
    Map::LookupResult found = nullptr;
    auto prev_it = end;

    __uint128_t num_threads_128 = num_threads;
    for (auto it = begin; it != end; ++it)
    {
        size_t hash = map.hash(*it);
        size_t cur_bucket = getThreadNum(hash, num_threads_128);
        if (cur_bucket != bucket)
            continue;
        if (prev_it != end && *it == *prev_it)
        {
            assert(found != nullptr);
            ++found->getMapped();
            continue;
        }
        prev_it = it;

        bool inserted;
        map.emplace(*it, found, inserted, hash);
        assert(found != nullptr);
        ++found->getMapped();
    }
}

static void aggregate2(MapTwoLevel & map, Source::const_iterator begin, Source::const_iterator end)
{
    for (auto it = begin; it != end; ++it)
        ++map[*it];
}

static void aggregate22(MapTwoLevel & map, Source::const_iterator begin, Source::const_iterator end)
{
    MapTwoLevel::LookupResult found = nullptr;
    auto prev_it = end;
    for (auto it = begin; it != end; ++it)
    {
        if (prev_it != end && *it == *prev_it)
        {
            assert(found != nullptr);
            ++found->getMapped();
            continue;
        }
        prev_it = it;

        bool inserted;
        map.emplace(*it, found, inserted);
        assert(found != nullptr);
        ++found->getMapped();
    }
}

#if !__clang__
#pragma GCC diagnostic pop
#endif

static void merge2(MapTwoLevel * maps, size_t num_threads, size_t bucket)
{
    for (size_t i = 1; i < num_threads; ++i)
        for (auto it = maps[i].impls[bucket].begin(); it != maps[i].impls[bucket].end(); ++it)
            maps[0].impls[bucket][it->getKey()] += it->getMapped();
}

static void aggregate3(Map & local_map, Map & global_map, Mutex & mutex, Source::const_iterator begin, Source::const_iterator end)
{
    static constexpr size_t threshold = 65536;

    for (auto it = begin; it != end; ++it)
    {
        if (local_map.size() < threshold)
        {
            ++local_map[*it];
            continue;
        }

        auto found = local_map.find(*it);
        if (found)
            ++found->getMapped();
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

static void aggregate33(Map & local_map, Map & global_map, Mutex & mutex, Source::const_iterator begin, Source::const_iterator end)
{
    static constexpr size_t threshold = 65536;

    for (auto it = begin; it != end; ++it)
    {
        Map::LookupResult found;
        bool inserted;
        local_map.emplace(*it, found, inserted);
        ++found->getMapped();

        if (inserted && local_map.size() == threshold)
        {
            std::lock_guard<Mutex> lock(mutex);
            for (auto & value_type : local_map)
                global_map[value_type.getKey()] += value_type.getMapped();

            local_map.clear();
        }
    }
}

static void aggregate4(Map & local_map, MapTwoLevel & global_map, Mutex * mutexes, Source::const_iterator begin, Source::const_iterator end)
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
                auto found = local_map.find(*it);

                if (found)
                    ++found->getMapped();
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

static void aggregate41(Map & local_map, MapTwoLevel & global_map, AlignedSmallLock * mutexes, Source::const_iterator begin, Source::const_iterator end)
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
                auto found = local_map.find(*it);

                if (found)
                    ++found->getMapped();
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

static void aggregate42(Map & local_map, MapTwoLevel512 & global_map, AlignedSmallLock * mutexes,
                             Source::const_iterator begin, Source::const_iterator end,
                             size_t threshold_buffer_size)
{
    static constexpr size_t threshold = 4096;
    static constexpr size_t block_size = 512;

    std::vector<std::vector<Key>> buffer(MapTwoLevel512::NUM_BUCKETS);
    std::vector<size_t> buffer_idx(MapTwoLevel512::NUM_BUCKETS);
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
                auto found = local_map.find(*it);

                if (found)
                    ++found->getMapped();
                else
                {
                    size_t hash_value = global_map.hash(*it);
                    size_t bucket = global_map.getBucketFromHash(hash_value);

                    if (buffer[bucket].size() - buffer_idx[bucket] >= threshold_buffer_size && mutexes[bucket].try_lock())
                    {
                        if (buffer_idx[bucket] < buffer[bucket].size())
                        {
                            for (size_t j = buffer_idx[bucket]; j < buffer[bucket].size(); ++j)
                                ++global_map.impls[bucket][buffer[bucket][j]];
                            buffer_idx[bucket] = buffer[bucket].size();
                        }
                        ++global_map.impls[bucket][*it];
                        mutexes[bucket].unlock();
                    }
                    else
                        buffer[bucket].push_back(*it);
                }
            }
        }
    }
    for (size_t i = 0; i < MapTwoLevel512::NUM_BUCKETS; ++i)
        for (size_t j = buffer_idx[i]; j < buffer[i].size(); ++j)
            ++local_map[buffer[i][j]];
}

static void aggregate43(Map & local_map,
                        Map & global_map,
                        std::vector<std::vector<std::list<Key>>> & buffer,
                        std::vector<std::vector<std::pair<std::list<Key>::iterator, std::list<Key>::iterator>>> & buffer_ranges,
                        std::vector<std::vector<AlignedSmallLockExt>> & mutexes,
                        Source::const_iterator begin,
                        Source::const_iterator end,
                        size_t cur_thread,
                        size_t num_threads)
{
    static constexpr size_t threshold = 16384;
    static constexpr size_t block_size = 8192;

    __uint128_t num_threads_128 = num_threads;
    auto it = begin;
    while (it != end)
    {
        auto block_end = std::min(end, it + block_size);

        if (local_map.size() < threshold)
        {
            for (; it != block_end; ++it)
            {
                size_t hash_value = global_map.hash(*it);
                size_t bucket = getThreadNum(hash_value, num_threads_128);

                if (bucket == cur_thread)
                    ++global_map[*it];
                else
                    ++local_map[*it];
            }
        }
        else
        {
            for (; it != block_end; ++it)
            {
                size_t hash_value = global_map.hash(*it);
                size_t bucket = getThreadNum(hash_value, num_threads_128);

                if (bucket == cur_thread)
                {
                    ++global_map[*it];
                    continue;
                }

                auto found = local_map.find(*it);
                if (found)
                    ++found->getMapped();
                else
                    buffer[cur_thread][bucket].push_back(*it);
            }

            for (size_t i = 0; i < num_threads; ++i)
                if (i != cur_thread && !buffer[cur_thread][i].empty() &&
                    buffer_ranges[cur_thread][i].second != std::prev(buffer[cur_thread][i].end()) &&
                    !mutexes[cur_thread][i].is_ready())
                {
                    buffer_ranges[cur_thread][i] = {std::next(buffer_ranges[cur_thread][i].second), std::prev(buffer[cur_thread][i].end())};
                    mutexes[cur_thread][i].set_ready();
                }
        }

        for (size_t i = 0; i < num_threads; ++i)
        {
            if (i != cur_thread && mutexes[i][cur_thread].is_ready())
            {
                auto iter = buffer_ranges[i][cur_thread].first;
                auto iter_end = buffer_ranges[i][cur_thread].second;
                while (iter != iter_end)
                {
                    ++global_map[*iter];
                    ++iter;
                }
                ++global_map[*iter];
                mutexes[i][cur_thread].set_done();
            }
        }
    }
}

static void merge43(Map & global_map,
                    std::vector<std::vector<std::list<Key>>> & buffers,
                    std::vector<std::vector<std::pair<std::list<Key>::iterator, std::list<Key>::iterator>>> & buffer_ranges,
                    std::vector<std::vector<AlignedSmallLockExt>> & mutexes,
                    size_t cur_thread,
                    size_t num_threads)
{
    for (size_t i = 0; i < num_threads; ++i)
        if (i != cur_thread && !buffers[i][cur_thread].empty() &&
            (buffer_ranges[i][cur_thread].second != std::prev(buffers[i][cur_thread].end()) || mutexes[i][cur_thread].is_ready()))
        {
            auto it = mutexes[i][cur_thread].is_ready() ? buffer_ranges[i][cur_thread].first : std::next(buffer_ranges[i][cur_thread].second);
            while (it != buffers[i][cur_thread].end())
            {
                ++global_map[*it];
                ++it;
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
    size_t n = std::stol(argv[1]);
    size_t num_threads = std::stol(argv[2]);
    size_t method = argc <= 3 ? 0 : std::stol(argv[3]);

    std::cerr << std::fixed << std::setprecision(2);

    ThreadPool pool(num_threads);

    Source data(n);

    {
        Stopwatch watch;
        DB::ReadBufferFromFileDescriptor in1(STDIN_FILENO);
        DB::CompressedReadBuffer in2(in1);

        for (size_t i = 0; i < n; ++i)
            DB::readBinary(data[i], in2);

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
            pool.scheduleOrThrowOnError([&, i] { aggregate1(
                std::ref(maps[i]),
                data.begin() + (data.size() * i) / num_threads,
                data.begin() + (data.size() * (i + 1)) / num_threads); });

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
                maps[0][it->getKey()] += it->getMapped();

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
            pool.scheduleOrThrowOnError([&, i] { aggregate12(
                                    std::ref(maps[i]),
                                    data.begin() + (data.size() * i) / num_threads,
                                    data.begin() + (data.size() * (i + 1)) / num_threads); });

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
                maps[0][it->getKey()] += it->getMapped();

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
            pool.scheduleOrThrowOnError([&, i] { aggregate1(
                std::ref(maps[i]),
                data.begin() + (data.size() * i) / num_threads,
                data.begin() + (data.size() * (i + 1)) / num_threads); });

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
                maps[0][iterators[i]->getKey()] += iterators[i]->getMapped();
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

    if (!method || method == 15)
    {
        /** Option 15.
         * Splitting-aggregator. Works vary fast when there are many different keys.
         */

        std::vector<Map> maps(num_threads);

        Stopwatch watch;

        size_t hash = maps[0].hash(data[0]);
        size_t min_hash = hash;
        size_t max_hash = min_hash;

        for (size_t i = 1; i < n; ++i)
        {
            hash = maps[0].hash(data[i]);
            max_hash = std::max(max_hash, hash);
            min_hash = std::min(min_hash, hash);
        }

        for (size_t i = 0; i < num_threads; ++i)
        {
            pool.scheduleOrThrowOnError([&, i] {
                aggregate15(
                    std::ref(maps[i]),
                    data.begin(),
                    data.end(),
                    min_hash + (max_hash - min_hash) / num_threads * i,
                    i + 1 == num_threads ? max_hash : min_hash + (max_hash - min_hash) / num_threads * (i + 1) - 1);
            });
        }

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

        double time_total = time_aggregated;
        std::cerr
            << "Total in " << time_total
            << " (" << n / time_total << " elem/sec.)"
            << std::endl;
        std::cerr << "Size: " << size_before_merge << std::endl << std::endl;
    }

    if (!method || method == 151)
    {
        /** Option 151.
         *  Splitting-aggregator with fast division. Works faster than method 15 in all cases.
         */

        std::vector<Map> maps(num_threads);

        Stopwatch watch;

        for (size_t i = 0; i < num_threads; ++i)
        {
            pool.scheduleOrThrowOnError([&, i] {
                aggregate151(
                    std::ref(maps[i]),
                    data.begin(),
                    data.end(),
                    num_threads,
                    i);
            });
        }

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

        double time_total = time_aggregated;
        std::cerr
            << "Total in " << time_total
            << " (" << n / time_total << " elem/sec.)"
            << std::endl;
        std::cerr << "Size: " << size_before_merge << std::endl << std::endl;
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
            pool.scheduleOrThrowOnError([&, i] { aggregate2(
                std::ref(maps[i]),
                data.begin() + (data.size() * i) / num_threads,
                data.begin() + (data.size() * (i + 1)) / num_threads); });

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
            pool.scheduleOrThrowOnError([&, i] { merge2(maps.data(), num_threads, i); });

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
            pool.scheduleOrThrowOnError([&, i] { aggregate22(
                                    std::ref(maps[i]),
                                    data.begin() + (data.size() * i) / num_threads,
                                    data.begin() + (data.size() * (i + 1)) / num_threads); });

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
            pool.scheduleOrThrowOnError([&, i] { merge2(maps.data(), num_threads, i); });

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
            pool.scheduleOrThrowOnError([&, i] { aggregate3(
                std::ref(local_maps[i]),
                std::ref(global_map),
                std::ref(mutex),
                data.begin() + (data.size() * i) / num_threads,
                data.begin() + (data.size() * (i + 1)) / num_threads); });

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
                global_map[it->getKey()] += it->getMapped();

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
            pool.scheduleOrThrowOnError([&, i] { aggregate33(
                std::ref(local_maps[i]),
                std::ref(global_map),
                std::ref(mutex),
                data.begin() + (data.size() * i) / num_threads,
                data.begin() + (data.size() * (i + 1)) / num_threads); });

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
                global_map[it->getKey()] += it->getMapped();

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
            pool.scheduleOrThrowOnError([&, i] { aggregate4(
                std::ref(local_maps[i]),
                std::ref(global_map),
                mutexes.data(),
                data.begin() + (data.size() * i) / num_threads,
                data.begin() + (data.size() * (i + 1)) / num_threads); });

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
                global_map[it->getKey()] += it->getMapped();

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

    if (!method || method == 41)
    {
        /** Option 4.
          * Same as method 4, but with atomic flags instead of mutexes. Works faster.
          */

        std::vector<Map> local_maps(num_threads);
        MapTwoLevel global_map;
        std::vector<AlignedSmallLock> mutexes(MapTwoLevel::NUM_BUCKETS);

        Stopwatch watch;

        for (size_t i = 0; i < num_threads; ++i)
            pool.scheduleOrThrowOnError([&, i] { aggregate41(
                std::ref(local_maps[i]),
                std::ref(global_map),
                mutexes.data(),
                data.begin() + (data.size() * i) / num_threads,
                data.begin() + (data.size() * (i + 1)) / num_threads); });

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
                global_map[it->getKey()] += it->getMapped();

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

    if (!method || method == 42)
    {
        /** Option 4.
          * Local hash tables with buffers + global shared table. Works very fast, especially when there are many
          * different keys.
          */

        std::vector<Map> local_maps(num_threads);
        std::vector<std::vector<std::vector<Key>>> buffers(num_threads, std::vector<std::vector<Key>>(MapTwoLevel512::NUM_BUCKETS));
        MapTwoLevel512 global_map;
        std::vector<AlignedSmallLock> mutexes(MapTwoLevel512::NUM_BUCKETS);

        Stopwatch watch;

        for (size_t i = 0; i < num_threads; ++i)
            pool.scheduleOrThrowOnError([&, i] { aggregate42(
                std::ref(local_maps[i]),
                std::ref(global_map),
                mutexes.data(),
                data.begin() + (data.size() * i) / num_threads,
                data.begin() + (data.size() * (i + 1)) / num_threads,
                n >= 70000000 ? 8 : 0); });

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

        /*for (size_t i = 0; i < MapTwoLevel4096::NUM_BUCKETS; ++i)
            std::cerr << global_map.impls[i].size() << ", ";*/
        std::cerr << std::endl;

        watch.restart();

        for (size_t i = 0; i < num_threads; ++i)
            for (auto it = local_maps[i].begin(); it != local_maps[i].end(); ++it)
                global_map[it->getKey()] += it->getMapped();

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

    if (!method || method == 43)
    {
        /** Option 43.
         *  Local hash tables with queues + Splitting-aggregator. Works rather fast but slower than method 42.
          */

        std::vector<Map> local_maps(num_threads);
        std::vector<Map> global_maps(num_threads);
        std::vector<std::vector<std::list<Key>>> buffers(num_threads, std::vector<std::list<Key>>(num_threads));
        std::vector<std::vector<std::pair<std::list<Key>::iterator, std::list<Key>::iterator>>> buffer_ranges(
            num_threads, std::vector<std::pair<std::list<Key>::iterator, std::list<Key>::iterator>>(num_threads));
        for (size_t i = 0; i < num_threads; ++i)
            for (size_t j = 0; j < num_threads; ++j)
                buffer_ranges[i][j] = {buffers[i][j].end(), buffers[i][j].end()};
        std::vector<std::vector<AlignedSmallLockExt>> mutexes(num_threads);
        for (size_t i = 0; i < num_threads; ++i)
            mutexes[i] = std::vector<AlignedSmallLockExt>(num_threads);

        Stopwatch watch;

        for (size_t i = 0; i < num_threads; ++i)
            pool.scheduleOrThrowOnError([&, i] { aggregate43(
                std::ref(local_maps[i]),
                std::ref(global_maps[i]),
                std::ref(buffers),
                std::ref(buffer_ranges),
                std::ref(mutexes),
                data.begin() + (data.size() * i) / num_threads,
                data.begin() + (data.size() * (i + 1)) / num_threads,
                i,
                num_threads); });

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

        std::cerr << "Sizes (global): ";
        size_t sum_size = 0;
        for (size_t i = 0; i < num_threads; ++i)
        {
            std::cerr << (i == 0 ? "" : ", ") << global_maps[i].size();
            sum_size += global_maps[i].size();
        }
        std::cerr << std::endl;
        std::cerr << "Size (global): " << sum_size << std::endl;
        size_before_merge += sum_size;

        watch.restart();

        __uint128_t num_threads_128 = num_threads;
        for (size_t i = 0; i < num_threads; ++i)
            for (auto it = local_maps[i].begin(); it != local_maps[i].end(); ++it)
            {
                size_t bucket_num = getThreadNum(it.getHash(), num_threads_128);
                global_maps[bucket_num][it->getKey()] += it->getMapped();
            }

        for (size_t i = 0; i < num_threads; ++i)
            pool.scheduleOrThrowOnError([&, i] { merge43(
                 std::ref(global_maps[i]),
                 std::ref(buffers),
                 std::ref(buffer_ranges),
                 std::ref(mutexes),
                 i,
                 num_threads); });

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

        sum_size = 0;
        for (size_t i = 0; i < num_threads; ++i)
            sum_size += global_maps[i].size();
        std::cerr << "Size: " << sum_size << std::endl << std::endl;
    }

    if (!method || method == 7)
    {
        /** Option 7.
         * The simplest baseline. One hash table with one thread and optimization for consecutive identical values.
         * Just to compare.
         */

        Map global_map;

        Stopwatch watch;

        aggregate12(global_map, data.begin(), data.end());

        watch.stop();
        double time_total = watch.elapsedSeconds();
        std::cerr
            << "Aggregated in " << time_total
            << " (" << n / time_total << " elem/sec.)"
            << std::endl;

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
            pool.scheduleOrThrowOnError([&] { aggregate5(
                std::ref(local_maps[i]),
                std::ref(global_map),
                data.begin() + (data.size() * i) / num_threads,
                data.begin() + (data.size() * (i + 1)) / num_threads); });

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
            pool.scheduleOrThrowOnError([&] { aggregate1(
                std::ref(maps[i]),
                data.begin() + (data.size() * i) / num_threads,
                data.begin() + (data.size() * (i + 1)) / num_threads); });

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
