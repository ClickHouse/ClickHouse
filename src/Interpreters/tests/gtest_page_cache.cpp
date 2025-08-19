#include <Common/CurrentThread.h>
#include <Common/PageCache.h>

#include <gtest/gtest.h>
#include <thread>

#ifdef OS_LINUX
#include <sys/sysinfo.h>
#endif

using namespace DB;

namespace ProfileEvents
{
    extern const Event PageCacheChunkMisses;
    extern const Event PageCacheChunkShared;
    extern const Event PageCacheChunkDataHits;
    extern const Event PageCacheChunkDataPartialHits;
    extern const Event PageCacheChunkDataMisses;
}

#define CHECK(x)                                                                           \
    do {                                                                                   \
    if (!(x))                                                                              \
        {                                                                                  \
            std::cerr << "check on line " << __LINE__ << " failed: " << #x << std::endl;   \
            std::abort();                                                                  \
        }                                                                                  \
    } while (false)

size_t estimateRAMSize()
{
#ifdef OS_LINUX
    struct sysinfo info;
    int r = sysinfo(&info);
    CHECK(r == 0);
    return static_cast<size_t>(info.totalram * info.mem_unit);
#else
    return 128ul << 30;
#endif
}

/// Do random reads and writes in PageCache from multiple threads, check that the data read matches the data written.
TEST(PageCache, DISABLED_Stress)
{
    /// There doesn't seem to be a reasonable way to simulate memory pressure or force the eviction of MADV_FREE-d pages.
    /// So we actually map more virtual memory than we have RAM and fill it all up a few times.
    /// This takes an eternity (a few minutes), but idk how else to hit MADV_FREE eviction.
    /// Expect ~1 GB/s, bottlenecked by page faults.
    size_t ram_size = estimateRAMSize();
    PageCache cache(2 << 20, 1 << 30, ram_size + ram_size / 10, /* use_madv_free */ true, /* use_huge_pages */ true);

    CHECK(cache.getResidentSetSize().page_cache_rss);

    const size_t num_keys = static_cast<size_t>(cache.maxChunks() * 1.5);
    const size_t pages_per_chunk = cache.chunkSize() / cache.pageSize();
    const size_t items_per_page = cache.pageSize() / 8;

    const size_t passes = 2;
    const size_t step = 20;
    const size_t num_threads = 20;
    const size_t chunks_touched = num_keys * passes * num_threads / step;
    std::atomic<size_t> progress {0};
    std::atomic<size_t> threads_finished {0};

    std::atomic<size_t> total_racing_writes {0};

    auto thread_func = [&]
    {
        pcg64 rng(randomSeed());
        std::vector<PinnedPageChunk> pinned;

        /// Stats.
        size_t racing_writes = 0;

        for (size_t i = 0; i < num_keys * passes; i += step)
        {
            progress += 1;

            /// Touch the chunks sequentially + noise (to increase interference across threads), or at random 10% of the time.
            size_t key_idx;
            if (rng() % 10 == 0)
                key_idx = std::uniform_int_distribution<size_t>(0, num_keys - 1)(rng);
            else
                key_idx = (i + std::uniform_int_distribution<size_t>(0, num_keys / 1000)(rng)) % num_keys;

            /// For some keys, always use detached_if_missing = true and check that cache always misses.
            bool key_detached_if_missing = key_idx % 100 == 42;
            bool detached_if_missing = key_detached_if_missing || i % 101 == 42;

            PageCacheKey key = key_idx * 0xcafebabeb0bad00dul; // a simple reversible hash (the constant can be any odd number)

            PinnedPageChunk chunk = cache.getOrSet(key, detached_if_missing, /* inject_eviction */ false);

            if (key_detached_if_missing)
                CHECK(!chunk.getChunk()->pages_populated.any());

            for (size_t page_idx = 0; page_idx < pages_per_chunk; ++page_idx)
            {
                bool populated = chunk.getChunk()->pages_populated.get(page_idx);
                /// Generate page contents deterministically from key and page index.
                size_t start = key_idx * page_idx;
                if (start % 37 == 13)
                {
                    /// Leave ~1/37 of the pages unpopulated.
                    CHECK(!populated);
                }
                else
                {
                    /// We may write/read the same memory from multiple threads in parallel here.
                    std::atomic<size_t> * items = reinterpret_cast<std::atomic<size_t> *>(chunk.getChunk()->data + cache.pageSize() * page_idx);
                    if (populated)
                    {
                        for (size_t j = 0; j < items_per_page; ++j)
                            CHECK(items[j].load(std::memory_order_relaxed) == start + j);
                    }
                    else
                    {
                        for (size_t j = 0; j < items_per_page; ++j)
                            items[j].store(start + j, std::memory_order_relaxed);
                        if (!chunk.markPagePopulated(page_idx))
                            racing_writes += 1;
                    }
                }
            }

            pinned.push_back(std::move(chunk));
            CHECK(cache.getPinnedSize() >= cache.chunkSize());
            /// Unpin 2 chunks on average.
            while (rng() % 3 != 0 && !pinned.empty())
            {
                size_t idx = rng() % pinned.size();
                if (idx != pinned.size() - 1)
                    pinned[idx] = std::move(pinned.back());
                pinned.pop_back();
            }
        }

        total_racing_writes += racing_writes;
        threads_finished += 1;
    };

    std::cout << fmt::format("doing {:.1f} passes over {:.1f} GiB of virtual memory\nthis will take a few minutes, progress printed every 10 seconds",
        chunks_touched * 1. / cache.maxChunks(), cache.maxChunks() * cache.chunkSize() * 1. / (1ul << 30)) << std::endl;

    auto start_time = std::chrono::steady_clock::now();

    std::vector<std::thread> threads;
    for (size_t i = 0; i < num_threads; ++i)
        threads.emplace_back(thread_func);

    for (size_t poll = 0;; ++poll)
    {
        if (threads_finished == num_threads)
            break;
        if (poll % 100 == 0)
            std::cout << fmt::format("{:.3f}%", progress.load() * 100. / num_keys / passes / num_threads * step) << std::endl;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    for (std::thread & t : threads)
        t.join();

    auto end_time = std::chrono::steady_clock::now();
    double elapsed_seconds = std::chrono::duration_cast<std::chrono::duration<double>>(end_time - start_time).count();
    double touched_gib = chunks_touched * cache.chunkSize() * 1. / (1ul << 30);
    std::cout << fmt::format("touched {:.1f} GiB in {:.1f} seconds, that's {:.3f} GiB/s",
        touched_gib, elapsed_seconds, touched_gib / elapsed_seconds) << std::endl;

    auto & counters = CurrentThread::getProfileEvents();

    std::cout << "stats:"
        << "\nchunk misses: " << counters[ProfileEvents::PageCacheChunkMisses].load()
        << "\nchunk shared: " << counters[ProfileEvents::PageCacheChunkShared].load()
        << "\nchunk data misses: " << counters[ProfileEvents::PageCacheChunkDataMisses].load()
        << "\nchunk data partial hits: " << counters[ProfileEvents::PageCacheChunkDataPartialHits].load()
        << "\nchunk data hits: " << counters[ProfileEvents::PageCacheChunkDataHits].load()
        << "\nracing page writes: " << total_racing_writes << std::endl;

    /// Check that we at least hit all the cases.
    CHECK(counters[ProfileEvents::PageCacheChunkMisses].load() > 0);
    CHECK(counters[ProfileEvents::PageCacheChunkShared].load() > 0);
    CHECK(counters[ProfileEvents::PageCacheChunkDataMisses].load() > 0);
    /// Partial hits are rare enough that sometimes this is zero, so don't check it.
    /// That's good news because we don't need to implement downloading parts of a chunk.
    /// CHECK(counters[ProfileEvents::PageCacheChunkDataPartialHits].load() > 0);
    CHECK(counters[ProfileEvents::PageCacheChunkDataHits].load() > 0);
    CHECK(total_racing_writes > 0);
    CHECK(cache.getPinnedSize() == 0);

    size_t rss = cache.getResidentSetSize().page_cache_rss;
    std::cout << "RSS: " << rss * 1. / (1ul << 30) << " GiB" << std::endl;
    /// This can be flaky if the system has < 10% free memory. If this turns out to be a problem, feel free to remove or reduce.
    CHECK(rss > ram_size / 10);

    cache.dropCache();

#ifdef OS_LINUX
    /// MADV_DONTNEED is not synchronous, and we're freeing lots of pages. Let's give Linux a lot of time.
    std::this_thread::sleep_for(std::chrono::seconds(10));
    size_t new_rss = cache.getResidentSetSize().page_cache_rss;
    std::cout << "RSS after dropping cache: " << new_rss * 1. / (1ul << 30) << " GiB" << std::endl;
    CHECK(new_rss < rss / 2);
#endif
}

/// Benchmark that measures the PageCache overhead for cache hits. Doesn't touch the actual data, so
/// memory bandwidth mostly doesn't factor into this.
/// This measures the overhead of things like madvise(MADV_FREE) and probing the pages (restoreChunkFromLimbo()).
/// Disabled in CI, run manually with --gtest_also_run_disabled_tests --gtest_filter=PageCache.DISABLED_HitsBench
TEST(PageCache, DISABLED_HitsBench)
{
    /// Do a few runs, with and without MADV_FREE.
    for (size_t num_threads = 1; num_threads <= 16; num_threads *= 2)
    {
        for (size_t run = 0; run < 8; ++ run)
        {
            bool use_madv_free = run % 2 == 1;
            bool use_huge_pages = run % 4 / 2 == 1;

            PageCache cache(2 << 20, 1ul << 30, 20ul << 30, use_madv_free, use_huge_pages);
            size_t passes = 3;
            std::atomic<size_t> total_misses {0};

            /// Prepopulate all chunks.
            for (size_t i = 0; i < cache.maxChunks(); ++i)
            {
                PageCacheKey key = i * 0xcafebabeb0bad00dul;
                PinnedPageChunk chunk = cache.getOrSet(key, /* detache_if_missing */ false, /* inject_eviction */ false);
                memset(chunk.getChunk()->data, 42, chunk.getChunk()->size);
                chunk.markPrefixPopulated(cache.chunkSize());
            }

            auto thread_func = [&]
            {
                pcg64 rng(randomSeed());
                size_t misses = 0;
                for (size_t i = 0; i < cache.maxChunks() * passes; ++i)
                {
                    PageCacheKey key = rng() % cache.maxChunks() * 0xcafebabeb0bad00dul;
                    PinnedPageChunk chunk = cache.getOrSet(key, /* detache_if_missing */ false, /* inject_eviction */ false);
                    if (!chunk.isPrefixPopulated(cache.chunkSize()))
                        misses += 1;
                }
                total_misses += misses;
            };

            auto start_time = std::chrono::steady_clock::now();

            std::vector<std::thread> threads;
            for (size_t i = 0; i < num_threads; ++i)
                threads.emplace_back(thread_func);

            for (std::thread & t : threads)
                t.join();

            auto end_time = std::chrono::steady_clock::now();
            double elapsed_seconds = std::chrono::duration_cast<std::chrono::duration<double>>(end_time - start_time).count();
            double fetched_gib = cache.chunkSize() * cache.maxChunks() * passes * 1. / (1ul << 30);
            std::cout << fmt::format(
                "threads {}, run {}, use_madv_free = {}, use_huge_pages = {}\nrequested {:.1f} GiB in {:.1f} seconds\n"
                "that's {:.1f} GiB/s, or overhead of {:.3}us/{:.1}MiB\n",
                num_threads, run, use_madv_free, use_huge_pages, fetched_gib, elapsed_seconds, fetched_gib / elapsed_seconds,
                elapsed_seconds * 1e6 / cache.maxChunks() / passes, cache.chunkSize() * 1. / (1 << 20)) << std::endl;

            if (total_misses != 0)
                std::cout << "!got " << total_misses.load() << " misses! perhaps your system doesn't have enough free memory, consider decreasing cache size in the benchmark code" << std::endl;
        }
    }
}
