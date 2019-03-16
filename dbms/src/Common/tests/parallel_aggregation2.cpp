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


template <typename Map>
struct AggregateIndependent
{
    template <typename Creator, typename Updater>
    static void NO_INLINE execute(const Source & data, size_t num_threads, std::vector<std::unique_ptr<Map>> & results,
                        Creator && creator, Updater && updater,
                        ThreadPool & pool)
    {
        results.reserve(num_threads);
        for (size_t i = 0; i < num_threads; ++i)
            results.emplace_back(new Map);

        for (size_t i = 0; i < num_threads; ++i)
        {
            auto begin = data.begin() + (data.size() * i) / num_threads;
            auto end = data.begin() + (data.size() * (i + 1)) / num_threads;
            auto & map = *results[i];

            pool.schedule([&, begin, end]()
            {
                for (auto it = begin; it != end; ++it)
                {
                    typename Map::iterator place;
                    bool inserted;
                    map.emplace(*it, place, inserted);

                    if (inserted)
                        creator(place->getSecond());
                    else
                        updater(place->getSecond());
                }
            });
        }

        pool.wait();
    }
};

#if !__clang__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#endif

template <typename Map>
struct AggregateIndependentWithSequentialKeysOptimization
{
    template <typename Creator, typename Updater>
    static void NO_INLINE execute(const Source & data, size_t num_threads, std::vector<std::unique_ptr<Map>> & results,
                        Creator && creator, Updater && updater,
                        ThreadPool & pool)
    {
        results.reserve(num_threads);
        for (size_t i = 0; i < num_threads; ++i)
            results.emplace_back(new Map);

        for (size_t i = 0; i < num_threads; ++i)
        {
            auto begin = data.begin() + (data.size() * i) / num_threads;
            auto end = data.begin() + (data.size() * (i + 1)) / num_threads;
            auto & map = *results[i];

            pool.schedule([&, begin, end]()
            {
                typename Map::iterator place;
                Key prev_key {};
                for (auto it = begin; it != end; ++it)
                {
                    if (it != begin && *it == prev_key)
                    {
                        updater(place->getSecond());
                        continue;
                    }
                    prev_key = *it;

                    bool inserted;
                    map.emplace(*it, place, inserted);

                    if (inserted)
                        creator(place->getSecond());
                    else
                        updater(place->getSecond());
                }
            });
        }

        pool.wait();
    }
};

#if !__clang__
#pragma GCC diagnostic pop
#endif


template <typename Map>
struct MergeSequential
{
    template <typename Merger>
    static void NO_INLINE execute(Map ** source_maps, size_t num_maps, Map *& result_map,
                        Merger && merger,
                        ThreadPool &)
    {
        for (size_t i = 1; i < num_maps; ++i)
        {
            auto begin = source_maps[i]->begin();
            auto end = source_maps[i]->end();
            for (auto it = begin; it != end; ++it)
                merger((*source_maps[0])[it->getFirst()], it->getSecond());
        }

        result_map = source_maps[0];
    }
};

template <typename Map>
struct MergeSequentialTransposed    /// In practice not better than usual.
{
    template <typename Merger>
    static void NO_INLINE execute(Map ** source_maps, size_t num_maps, Map *& result_map,
                        Merger && merger,
                        ThreadPool &)
    {
        std::vector<typename Map::iterator> iterators(num_maps);
        for (size_t i = 1; i < num_maps; ++i)
            iterators[i] = source_maps[i]->begin();

        result_map = source_maps[0];

        while (true)
        {
            bool finish = true;
            for (size_t i = 1; i < num_maps; ++i)
            {
                if (iterators[i] == source_maps[i]->end())
                    continue;

                finish = false;
                merger((*result_map)[iterators[i]->getFirst()], iterators[i]->getSecond());
                ++iterators[i];
            }

            if (finish)
                break;
        }
    }
};

template <typename Map, typename ImplMerge>
struct MergeParallelForTwoLevelTable
{
    template <typename Merger>
    static void NO_INLINE execute(Map ** source_maps, size_t num_maps, Map *& result_map,
                        Merger && merger,
                        ThreadPool & pool)
    {
        for (size_t bucket = 0; bucket < Map::NUM_BUCKETS; ++bucket)
            pool.schedule([&, bucket, num_maps]
            {
                std::vector<typename Map::Impl *> section(num_maps);
                for (size_t i = 0; i < num_maps; ++i)
                    section[i] = &source_maps[i]->impls[bucket];

                typename Map::Impl * res;
                ImplMerge::execute(section.data(), num_maps, res, merger, pool);
            });

        pool.wait();
        result_map = source_maps[0];
    }
};


template <typename Map, typename Aggregate, typename Merge>
struct Work
{
    template <typename Creator, typename Updater, typename Merger>
    static void NO_INLINE execute(const Source & data, size_t num_threads,
                        Creator && creator, Updater && updater, Merger && merger,
                        ThreadPool & pool)
    {
        std::vector<std::unique_ptr<Map>> intermediate_results;

        Stopwatch watch;

        Aggregate::execute(data, num_threads, intermediate_results, std::forward<Creator>(creator), std::forward<Updater>(updater), pool);
        size_t num_maps = intermediate_results.size();

        watch.stop();
        double time_aggregated = watch.elapsedSeconds();
        std::cerr
            << "Aggregated in " << time_aggregated
            << " (" << data.size() / time_aggregated << " elem/sec.)"
            << std::endl;

        size_t size_before_merge = 0;
        std::cerr << "Sizes: ";
        for (size_t i = 0; i < num_threads; ++i)
        {
            std::cerr << (i == 0 ? "" : ", ") << intermediate_results[i]->size();
            size_before_merge += intermediate_results[i]->size();
        }
        std::cerr << std::endl;

        watch.restart();

        std::vector<Map*> intermediate_results_ptrs(num_maps);
        for (size_t i = 0; i < num_maps; ++i)
            intermediate_results_ptrs[i] = intermediate_results[i].get();

        Map * result_map;
        Merge::execute(intermediate_results_ptrs.data(), num_maps, result_map, std::forward<Merger>(merger), pool);

        watch.stop();
        double time_merged = watch.elapsedSeconds();
        std::cerr
            << "Merged in " << time_merged
            << " (" << size_before_merge / time_merged << " elem/sec.)"
            << std::endl;

        double time_total = time_aggregated + time_merged;
        std::cerr
            << "Total in " << time_total
            << " (" << data.size() / time_total << " elem/sec.)"
            << std::endl;
        std::cerr << "Size: " << result_map->size() << std::endl << std::endl;
    }
};


using Map = HashMap<Key, Value, HashCRC32<Key>>;
using MapTwoLevel = TwoLevelHashMap<Key, Value, HashCRC32<Key>>;
using Mutex = std::mutex;


struct Creator
{
    void operator()(Value &) const {}
};

#if !__clang__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#endif

struct Updater
{
    void operator()(Value & x) const { ++x; }
};

#if !__clang__
#pragma GCC diagnostic pop
#endif

struct Merger
{
    void operator()(Value & dst, const Value & src) const { dst += src; }
};



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

    Creator creator;
    Updater updater;
    Merger merger;

    if (!method || method == 1)
        Work<
            Map,
            AggregateIndependent<Map>,
            MergeSequential<Map>
        >::execute(data, num_threads, creator, updater, merger, pool);

    if (!method || method == 2)
        Work<
            Map,
            AggregateIndependentWithSequentialKeysOptimization<Map>,
            MergeSequential<Map>
        >::execute(data, num_threads, creator, updater, merger, pool);

    if (!method || method == 3)
        Work<
            Map,
            AggregateIndependent<Map>,
            MergeSequentialTransposed<Map>
        >::execute(data, num_threads, creator, updater, merger, pool);

    if (!method || method == 4)
        Work<
            Map,
            AggregateIndependentWithSequentialKeysOptimization<Map>,
            MergeSequentialTransposed<Map>
        >::execute(data, num_threads, creator, updater, merger, pool);

    if (!method || method == 5)
        Work<
            MapTwoLevel,
            AggregateIndependent<MapTwoLevel>,
            MergeSequential<MapTwoLevel>
        >::execute(data, num_threads, creator, updater, merger, pool);

    if (!method || method == 6)
        Work<
            MapTwoLevel,
            AggregateIndependentWithSequentialKeysOptimization<MapTwoLevel>,
            MergeSequential<MapTwoLevel>
        >::execute(data, num_threads, creator, updater, merger, pool);

    if (!method || method == 7)
        Work<
            MapTwoLevel,
            AggregateIndependent<MapTwoLevel>,
            MergeSequentialTransposed<MapTwoLevel>
        >::execute(data, num_threads, creator, updater, merger, pool);

    if (!method || method == 8)
        Work<
            MapTwoLevel,
            AggregateIndependentWithSequentialKeysOptimization<MapTwoLevel>,
            MergeSequentialTransposed<MapTwoLevel>
        >::execute(data, num_threads, creator, updater, merger, pool);

    if (!method || method == 9)
        Work<
            MapTwoLevel,
            AggregateIndependent<MapTwoLevel>,
            MergeParallelForTwoLevelTable<MapTwoLevel, MergeSequential<MapTwoLevel::Impl>>
        >::execute(data, num_threads, creator, updater, merger, pool);

    if (!method || method == 10)
        Work<
            MapTwoLevel,
            AggregateIndependentWithSequentialKeysOptimization<MapTwoLevel>,
            MergeParallelForTwoLevelTable<MapTwoLevel, MergeSequential<MapTwoLevel::Impl>>
        >::execute(data, num_threads, creator, updater, merger, pool);

    if (!method || method == 13)
        Work<
            MapTwoLevel,
            AggregateIndependent<MapTwoLevel>,
            MergeParallelForTwoLevelTable<MapTwoLevel, MergeSequentialTransposed<MapTwoLevel::Impl>>
        >::execute(data, num_threads, creator, updater, merger, pool);

    if (!method || method == 14)
        Work<
            MapTwoLevel,
            AggregateIndependentWithSequentialKeysOptimization<MapTwoLevel>,
            MergeParallelForTwoLevelTable<MapTwoLevel, MergeSequentialTransposed<MapTwoLevel::Impl>>
        >::execute(data, num_threads, creator, updater, merger, pool);

    return 0;
}
