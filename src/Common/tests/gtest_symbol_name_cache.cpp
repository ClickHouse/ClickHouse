#include <gtest/gtest.h>

#include <Common/SymbolNameCache.h>

#include <algorithm>
#include <atomic>
#include <string>
#include <thread>
#include <vector>

using DB::SymbolNameCache;

namespace
{

/// Far more offers than admission needs on average (`ADMISSION_ONE_IN`).
constexpr size_t MAX_OFFERS = 200;

/// Never offered to a cache: lookups of it only mark that lookups happen.
constexpr UInt32 ABSENT_KEY = 0xFFFFFF00;

std::string nameFor(UInt32 key)
{
    std::string name = "symbol_" + std::to_string(key) + "_";
    name.append(key % 97, static_cast<char>('a' + key % 26));
    return name;
}

bool lookup(SymbolNameCache & cache, UInt32 key, UInt64 now, std::string & name)
{
    char buf[256];
    auto size = cache.tryGet(key, now, buf, sizeof(buf));
    if (!size)
        return false;
    EXPECT_EQ(buf[*size], 0);
    name.assign(buf, *size);
    return true;
}

/// What a user of the cache does: look up, and on a miss decode the name and offer it to the cache.
/// Returns whether it was a hit.
bool touch(SymbolNameCache & cache, UInt32 key, UInt64 now)
{
    std::string name;
    if (lookup(cache, key, now, name))
    {
        EXPECT_EQ(name, nameFor(key));
        return true;
    }
    cache.add(key, nameFor(key), now);
    return false;
}

/// Touches the key until it is cached, at most `MAX_OFFERS` times. Returns whether it is cached.
bool touchUntilCached(SymbolNameCache & cache, UInt32 key, UInt64 now)
{
    for (size_t i = 0; i <= MAX_OFFERS; ++i)
        if (touch(cache, key, now))
            return true;
    return false;
}

/// Touches every key of `[0, num_keys)` once per second for `num_rounds` seconds, calling maintenance
/// after each round if asked. Returns the number of hits in the last round.
size_t touchRounds(SymbolNameCache & cache, UInt32 num_keys, UInt64 & now, size_t num_rounds, bool maintain)
{
    size_t hits = 0;
    for (size_t round = 0; round < num_rounds; ++round, ++now)
    {
        hits = 0;
        for (UInt32 key = 0; key < num_keys; ++key)
            hits += touch(cache, key, now);
        if (maintain)
            cache.maintain(now);
    }
    return hits;
}

/// Touches every second a working set that grows by `step` keys per second up to `[0, num_keys)`, as
/// a server warming up does, calling maintenance after each round if asked.
void growWorkingSet(SymbolNameCache & cache, UInt32 num_keys, UInt32 step, UInt64 & now, bool maintain)
{
    for (UInt32 size = step;; size += step)
    {
        UInt32 current = std::min(size, num_keys);
        touchRounds(cache, current, now, 1, maintain);
        if (current == num_keys)
            return;
    }
}

/// Lookups of a name that is never cached, one per second for `seconds` seconds: time passes for
/// the cache while none of its entries is used.
void idleSeconds(SymbolNameCache & cache, UInt64 & now, size_t seconds, bool maintain)
{
    for (size_t i = 0; i < seconds; ++i, ++now)
    {
        std::string name;
        EXPECT_FALSE(lookup(cache, ABSENT_KEY, now, name));
        if (maintain)
            cache.maintain(now);
    }
}

/// Bytes of the names of the keys of `[0, num_keys)` that are cached, with their length headers.
size_t bytesOfCachedNames(SymbolNameCache & cache, UInt32 num_keys, UInt64 now)
{
    size_t bytes = 0;
    for (UInt32 key = 0; key < num_keys; ++key)
    {
        std::string name;
        if (lookup(cache, key, now, name))
            bytes += sizeof(UInt32) + name.size();
    }
    return bytes;
}

/// The table has at least a key, a stamp and a name pointer per entry.
constexpr size_t MIN_TABLE_BYTES_PER_ENTRY = 2 * sizeof(UInt32) + sizeof(void *);

class SymbolNameCacheWithHooks : public SymbolNameCache
{
public:
    using SymbolNameCache::SymbolNameCache;
    using SymbolNameCache::getBucketIndexOfKey;
    using SymbolNameCache::getNumReaderSlots;
    using SymbolNameCache::holdReaderSlot;
    using SymbolNameCache::releaseReaderSlot;
};

/// The key and further keys of its bucket, `count` in all.
std::vector<UInt32> keysOfBucket(const SymbolNameCacheWithHooks & cache, UInt32 first_key, size_t count)
{
    std::vector<UInt32> keys{first_key};
    for (UInt32 key = first_key + 1; keys.size() < count; ++key)
        if (cache.getBucketIndexOfKey(key) == cache.getBucketIndexOfKey(first_key))
            keys.push_back(key);
    return keys;
}

}

TEST(SymbolNameCache, AdmissionThroughRepeatedOffers)
{
    SymbolNameCache cache(1024, 1000000);
    const std::string expected = nameFor(42);
    std::string name;

    EXPECT_FALSE(lookup(cache, 42, 0, name));
    EXPECT_EQ(cache.capacity(), 0);
    EXPECT_EQ(cache.bytesAllocated(), 0);

    size_t offers = 0;
    while (cache.size() == 0 && offers < MAX_OFFERS)
    {
        cache.add(42, expected, 0);
        ++offers;
    }
    ASSERT_EQ(cache.size(), 1);
    EXPECT_EQ(cache.capacity(), 1024);
    EXPECT_EQ(cache.writerPasses(), 1);
    EXPECT_GE(cache.bytesAllocated(), sizeof(UInt32) + expected.size() + 1024 * MIN_TABLE_BYTES_PER_ENTRY);
    ASSERT_TRUE(lookup(cache, 42, 0, name));
    EXPECT_EQ(name, expected);

    /// The name and its NUL must fit.
    std::vector<char> buf(expected.size() + 1);
    EXPECT_FALSE(cache.tryGet(42, 0, buf.data(), expected.size()));
    auto size = cache.tryGet(42, 0, buf.data(), buf.size());
    ASSERT_TRUE(size);
    EXPECT_EQ(std::string(buf.data(), *size), expected);
    EXPECT_EQ(buf[*size], 0);

    /// A present key does not take the writer path.
    const size_t bytes = cache.bytesAllocated();
    for (size_t i = 0; i < MAX_OFFERS; ++i)
        cache.add(42, expected, 0);
    EXPECT_EQ(cache.size(), 1);
    EXPECT_EQ(cache.writerPasses(), 1);
    EXPECT_EQ(cache.bytesAllocated(), bytes);
}

TEST(SymbolNameCache, AdmitsOneOfferInEight)
{
    /// Large enough that no bucket fills up, so that every admitted offer is cached.
    SymbolNameCache cache(1 << 17, 1 << 20);
    const UInt32 offers = 16000;
    for (UInt32 key = 1000; key < 1000 + offers; ++key)
        cache.add(key, nameFor(key), 0);
    const size_t expected = offers / SymbolNameCache::ADMISSION_ONE_IN;
    EXPECT_GT(cache.size(), expected * 8 / 10);
    EXPECT_LT(cache.size(), expected * 12 / 10);
}

TEST(SymbolNameCache, EvictionOrder)
{
    SymbolNameCacheWithHooks cache(1024, 1 << 20);
    UInt64 now = 1;
    ASSERT_TRUE(touchUntilCached(cache, 0, now));

    /// Eight names of one bucket, used one second apart, so that the first one is used least recently.
    const std::vector<UInt32> keys = keysOfBucket(cache, 0, SymbolNameCache::BUCKET_SIZE + 2);
    const UInt32 new_name = keys[8];
    const UInt32 newer_name = keys[9];
    for (size_t i = 0; i < SymbolNameCache::BUCKET_SIZE; ++i)
        ASSERT_TRUE(touchUntilCached(cache, keys[i], now));
    for (size_t i = 0; i < SymbolNameCache::BUCKET_SIZE; ++i)
        ASSERT_TRUE(touch(cache, keys[i], ++now));

    /// While they are recently used, a ninth name is not cached.
    EXPECT_FALSE(touchUntilCached(cache, new_name, now));

    /// Once lookups of other names have gone on for longer than the window, the entry used least
    /// recently makes room for it.
    idleSeconds(cache, ++now, SymbolNameCache::RECENT_WINDOW_SECONDS + 1, false);
    EXPECT_TRUE(touchUntilCached(cache, new_name, now));
    std::string name;
    EXPECT_FALSE(lookup(cache, keys[0], now, name));

    /// A never used entry goes before used ones that are no longer recently used, although it is newer.
    ++now;
    EXPECT_TRUE(touchUntilCached(cache, newer_name, now));
    EXPECT_FALSE(lookup(cache, new_name, now, name));
    for (size_t i = 1; i < SymbolNameCache::BUCKET_SIZE; ++i)
        EXPECT_TRUE(lookup(cache, keys[i], now, name)) << i;
    EXPECT_EQ(cache.capacity(), 1024);
}

TEST(SymbolNameCache, GrowsOnlyWhenQuarterIsRecentlyUsed)
{
    SymbolNameCacheWithHooks cache(1024, 1 << 20);
    UInt64 now = 1;
    ASSERT_TRUE(touchUntilCached(cache, 0, now));

    /// Eight names of one bucket, used (hit a second after the insert).
    std::vector<UInt32> same_bucket = keysOfBucket(cache, 0, SymbolNameCache::BUCKET_SIZE + 1);
    const UInt32 extra = same_bucket.back();
    same_bucket.pop_back();
    for (UInt32 key : same_bucket)
        ASSERT_TRUE(touchUntilCached(cache, key, now));
    ++now;
    for (UInt32 key : same_bucket)
        ASSERT_TRUE(touch(cache, key, now));

    /// The ninth name of the bucket does not fit: a recently used entry is not evicted, and far less
    /// than a quarter of the capacity is recently used.
    EXPECT_FALSE(touchUntilCached(cache, extra, now));
    EXPECT_EQ(cache.capacity(), 1024);

    /// Steady state: the uncached name keeps being offered while the bucket keeps being used. It
    /// neither scans the table nor takes the writer path.
    const size_t passes = cache.writerPasses();
    const size_t scans = cache.tableScans();
    for (size_t second = 0; second < 30; ++second)
    {
        ++now;
        for (UInt32 key : same_bucket)
            EXPECT_TRUE(touch(cache, key, now));
        EXPECT_FALSE(touchUntilCached(cache, extra, now));
    }
    EXPECT_EQ(cache.writerPasses(), passes);
    EXPECT_EQ(cache.tableScans(), scans);

    /// Names that are inserted but not used do not count.
    std::vector<UInt32> others;
    for (UInt32 key = 100000; others.size() < 1024 / 4; ++key)
        if (cache.getBucketIndexOfKey(key) != cache.getBucketIndexOfKey(0) && touchUntilCached(cache, key, now))
            others.push_back(key);
    EXPECT_FALSE(touchUntilCached(cache, extra, now));
    EXPECT_EQ(cache.capacity(), 1024);
    EXPECT_EQ(cache.tableScans(), scans);

    /// Once they are used and a quarter of the capacity is recently used, the next offer of the name
    /// counts the recently used entries once and grows the table.
    ++now;
    for (UInt32 key : others)
        touch(cache, key, now);
    for (UInt32 key : same_bucket)
        EXPECT_TRUE(touch(cache, key, now));
    touchUntilCached(cache, extra, now);
    EXPECT_EQ(cache.capacity(), 2048);
    EXPECT_EQ(cache.tableScans(), scans + 1);
}

TEST(SymbolNameCache, OneOffScanDoesNotGrowOrEvictRecentlyUsed)
{
    SymbolNameCache cache(8192, 1 << 20);
    const UInt32 hot = 1000;
    UInt64 now = 0;
    growWorkingSet(cache, hot, 100, now, true);
    touchRounds(cache, hot, now, 150, true);
    ASSERT_EQ(touchRounds(cache, hot, now, 1, true), hot);

    /// One pass over many distinct names, while the hot names go on being used.
    const UInt32 names_per_second = 20000;
    for (UInt32 i = 0; i < 400000; ++i)
    {
        touch(cache, 1000000 + i, now);
        cache.maintain(now);
        if ((i + 1) % names_per_second == 0)
            touchRounds(cache, hot, now, 1, true);
    }

    EXPECT_EQ(cache.capacity(), 8192);
    EXPECT_LE(cache.size(), cache.capacity());
    EXPECT_EQ(touchRounds(cache, hot, now, 1, true), hot);
}

TEST(SymbolNameCache, RepeatedPassesAreReuse)
{
    /// A name looked up again in a later second is reused, so that repeated passes over the same
    /// names, a few seconds apart, size the table like any other working set.
    SymbolNameCache cache(8192, 1 << 20);
    UInt64 now = 0;
    const UInt32 names_per_second = 10000;
    size_t lookups = 0;
    for (size_t pass = 0; pass < 10; ++pass)
    {
        for (UInt32 key = 0; key < 20000; ++key)
        {
            touch(cache, key, now);
            cache.maintain(now);
            if (++lookups % names_per_second == 0)
                ++now;
        }
    }
    EXPECT_GT(cache.capacity(), 8192);
}

TEST(SymbolNameCache, GrowsForLargeWorkingSetAndSettles)
{
    SymbolNameCache cache(1024, 1 << 20);
    const UInt32 working_set = 3000;
    UInt64 now = 0;

    /// Long enough for admission, which takes `ADMISSION_ONE_IN` offers on average, to complete.
    growWorkingSet(cache, working_set, 200, now, true);
    size_t hits = touchRounds(cache, working_set, now, 150, true);
    const size_t capacity = cache.capacity();
    EXPECT_GT(capacity, working_set);
    EXPECT_GE(hits, working_set * 98 / 100);

    /// Neither growth nor shrinking nor any writer pass any more.
    const size_t passes = cache.writerPasses();
    EXPECT_EQ(touchRounds(cache, working_set, now, 5 * SymbolNameCache::MAINTENANCE_INTERVAL_SECONDS, true), hits);
    EXPECT_EQ(cache.capacity(), capacity);
    EXPECT_EQ(cache.writerPasses(), passes);
}

TEST(SymbolNameCache, GrowthStopsAtNaturalLimit)
{
    /// Twice the number of names, rounded up to a power of two; without the limit the same names
    /// make the table grow further.
    SymbolNameCache cache(1024, 5000);
    SymbolNameCache unlimited_cache(1024, 1 << 20);
    UInt64 now = 0;
    UInt64 unlimited_now = 0;
    growWorkingSet(cache, 5000, 250, now, false);
    touchRounds(cache, 5000, now, 60, false);
    growWorkingSet(unlimited_cache, 5000, 250, unlimited_now, false);
    touchRounds(unlimited_cache, 5000, unlimited_now, 60, false);
    EXPECT_EQ(cache.capacity(), 16384);
    EXPECT_GT(unlimited_cache.capacity(), 16384);
    EXPECT_LE(cache.size(), cache.capacity());

    /// The minimum wins over the limit.
    SymbolNameCache small_cache(64, 10);
    touchRounds(small_cache, 1000, now, 30, false);
    EXPECT_EQ(small_cache.capacity(), 64);
}

TEST(SymbolNameCache, MaintenanceShrinksAfterWorkingSetDrops)
{
    SymbolNameCache cache(1024, 1 << 20);
    UInt64 now = 0;
    growWorkingSet(cache, 12288, 512, now, true);
    touchRounds(cache, 12288, now, 60, true);
    const size_t grown_capacity = cache.capacity();
    const size_t grown_bytes = cache.bytesAllocated();
    ASSERT_GE(grown_capacity, 4 * 12288);

    /// Only a small part stays hot for longer than the window and a maintenance interval:
    /// the table is rebuilt for eight times the recently used entries.
    const UInt32 hot = 1000;
    size_t hits = touchRounds(cache, hot, now, 2 * SymbolNameCache::MAINTENANCE_INTERVAL_SECONDS + 10, true);
    EXPECT_EQ(cache.capacity(), 8192);
    EXPECT_GE(hits, hot * 98 / 100);
    EXPECT_EQ(cache.size(), hits);
    const size_t table_bytes = cache.bytesAllocated() - bytesOfCachedNames(cache, hot, now);
    EXPECT_GE(table_bytes, cache.capacity() * MIN_TABLE_BYTES_PER_ENTRY);
    EXPECT_LT(cache.bytesAllocated(), grown_bytes);

    /// Hits on the kept entries neither grow nor shrink it further.
    const size_t passes = cache.writerPasses();
    EXPECT_EQ(touchRounds(cache, hot, now, 5 * SymbolNameCache::MAINTENANCE_INTERVAL_SECONDS, true), hits);
    EXPECT_EQ(cache.capacity(), 8192);
    EXPECT_EQ(cache.writerPasses(), passes);

    /// Lookups go on, but none of the cached names is used any more: down to the minimum, and
    /// only the table is left.
    idleSeconds(cache, now, 3 * SymbolNameCache::MAINTENANCE_INTERVAL_SECONDS, true);
    EXPECT_EQ(cache.capacity(), 1024);
    EXPECT_EQ(cache.size(), 0);
    EXPECT_EQ(cache.bytesAllocated() * 8192, table_bytes * 1024);
}

TEST(SymbolNameCache, PauseBetweenBurstsKeepsEntries)
{
    SymbolNameCacheWithHooks cache(1024, 1 << 20);
    const UInt32 names = 6000;
    const UInt32 names_per_second = 2000;
    UInt64 now = 0;

    /// Bursts of four passes over the same names, an hour apart, with maintenance after each lookup.
    /// If asked, no writer can take the table during the first second of the burst, so that the
    /// maintenance due at its start is deferred.
    auto burst = [&](bool block_writers_in_first_second, size_t & first_pass_hits)
    {
        first_pass_hits = 0;
        const size_t held_slot = cache.getNumReaderSlots() - 1;
        if (block_writers_in_first_second)
            cache.holdReaderSlot(held_slot);
        size_t lookups = 0;
        for (size_t pass = 0; pass < 4; ++pass)
        {
            for (UInt32 key = 0; key < names; ++key)
            {
                bool hit = touch(cache, key, now);
                cache.maintain(now);
                if (pass == 0)
                    first_pass_hits += hit;
                if (++lookups % names_per_second == 0)
                {
                    ++now;
                    if (block_writers_in_first_second && lookups == names_per_second)
                        cache.releaseReaderSlot(held_slot);
                }
            }
        }
        now += 3600;
    };

    size_t first_pass_hits = 0;
    for (size_t i = 0; i < 20; ++i)
        burst(false, first_pass_hits);
    const size_t capacity = cache.capacity();
    const size_t passes = cache.writerPasses();

    burst(false, first_pass_hits);
    EXPECT_GE(first_pass_hits, names * 98 / 100);
    EXPECT_EQ(cache.capacity(), capacity);
    EXPECT_LE(cache.writerPasses() - passes, names / 100);

    burst(true, first_pass_hits);
    EXPECT_GE(first_pass_hits, names * 98 / 100);
    EXPECT_EQ(cache.capacity(), capacity);
    burst(false, first_pass_hits);
    EXPECT_GE(first_pass_hits, names * 98 / 100);
    EXPECT_EQ(cache.capacity(), capacity);
}

TEST(SymbolNameCache, MaintenanceRetriesOnlyWhenDue)
{
    SymbolNameCacheWithHooks cache(256, 1 << 20);
    UInt64 now = 0;
    growWorkingSet(cache, 2000, 100, now, false);
    touchRounds(cache, 2000, now, 30, false);
    const size_t capacity = cache.capacity();
    const size_t size = cache.size();
    ASSERT_GT(capacity, 256);

    /// Lookups go on without using any entry for longer than the window: maintenance wants to shrink,
    /// but a reader never leaves.
    idleSeconds(cache, now, SymbolNameCache::RECENT_WINDOW_SECONDS + 1, false);
    const size_t held_slot = cache.getNumReaderSlots() - 1;
    cache.holdReaderSlot(held_slot);
    cache.maintain(now);
    EXPECT_EQ(cache.capacity(), capacity);
    cache.releaseReaderSlot(held_slot);

    /// Not due again until the retry delay passes.
    cache.maintain(now);
    EXPECT_EQ(cache.capacity(), capacity);
    EXPECT_EQ(cache.size(), size);

    cache.maintain(now + SymbolNameCache::MAINTENANCE_RETRY_SECONDS);
    EXPECT_EQ(cache.capacity(), 256);
    EXPECT_EQ(cache.size(), 0);
}

TEST(SymbolNameCache, ReadersSharingSlotDoNotMiss)
{
    SymbolNameCacheWithHooks cache(1024, 1000);
    ASSERT_TRUE(touchUntilCached(cache, 1, 0));

    /// Whatever CPU the lookup runs on, its slot already has two readers inside.
    for (size_t i = 0; i < cache.getNumReaderSlots(); ++i)
    {
        cache.holdReaderSlot(i);
        cache.holdReaderSlot(i);
    }
    std::string name;
    EXPECT_TRUE(lookup(cache, 1, 0, name));
    EXPECT_EQ(name, nameFor(1));
    for (size_t i = 0; i < cache.getNumReaderSlots(); ++i)
    {
        cache.releaseReaderSlot(i);
        cache.releaseReaderSlot(i);
    }
}

TEST(SymbolNameCache, InsertGivesUpWhileReaderNeverLeaves)
{
    SymbolNameCacheWithHooks cache(1024, 1000);
    const size_t held_slot = cache.getNumReaderSlots() - 1;
    cache.holdReaderSlot(held_slot);

    for (size_t i = 0; i < MAX_OFFERS; ++i)
        cache.add(1, nameFor(1), 0);
    EXPECT_EQ(cache.size(), 0);
    EXPECT_EQ(cache.writerPasses(), 0);

    cache.releaseReaderSlot(held_slot);
    EXPECT_TRUE(touchUntilCached(cache, 1, 0));
    EXPECT_EQ(cache.size(), 1);
    EXPECT_EQ(cache.writerPasses(), 1);
}

TEST(SymbolNameCache, ConcurrentLookupsInsertsAndMaintenance)
{
    SymbolNameCache cache(1024, 1 << 13);

    const size_t num_threads = 8;
    const size_t ops_per_thread = 200000;
    std::atomic<UInt64> total_ops{0};
    std::atomic<size_t> wrong_names{0};
    std::atomic<size_t> total_hits{0};

    auto worker = [&](size_t thread_num)
    {
        UInt64 rng = 0x9E3779B97F4A7C15ULL * (thread_num + 1);
        char buf[256];
        size_t hits = 0;
        for (size_t i = 0; i < ops_per_thread; ++i)
        {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;

            /// The time advances quickly and the hot set alternates between large and small, so that
            /// the table grows, evicts and shrinks during the run. Some keys are cold; the buffer is
            /// sometimes too small.
            UInt64 op = total_ops.fetch_add(1, std::memory_order_relaxed);
            UInt64 now = op / 1000;
            UInt32 hot_set = ((now / 100) % 2) ? 300 : 6000;
            UInt32 key = (rng % 64 != 0) ? static_cast<UInt32>((rng >> 8) % hot_set) : static_cast<UInt32>((rng >> 8) % 30000);
            size_t buf_size = ((rng >> 40) % 8 == 0) ? 32 : sizeof(buf);

            auto size = cache.tryGet(key, now, buf, buf_size);
            if (size)
            {
                ++hits;
                if (buf[*size] != 0 || std::string_view(buf, *size) != nameFor(key))
                    wrong_names.fetch_add(1, std::memory_order_relaxed);
            }
            else
            {
                cache.add(key, nameFor(key), now);
            }
            cache.maintain(now);
        }
        total_hits.fetch_add(hits, std::memory_order_relaxed);
    };

    std::vector<std::thread> threads;
    threads.reserve(num_threads);
    for (size_t i = 0; i < num_threads; ++i)
        threads.emplace_back(worker, i);
    for (auto & thread : threads)
        thread.join();

    EXPECT_EQ(wrong_names.load(), 0);
    EXPECT_GT(total_hits.load(), 0);
    EXPECT_GT(cache.writerPasses(), 0);
    EXPECT_LE(cache.size(), cache.capacity());
    EXPECT_LE(cache.capacity(), 1 << 14);

    /// Still consistent and usable after the stress.
    UInt64 now = total_ops.load() / 1000 + 1;
    for (UInt32 key = 0; key < 100; ++key)
    {
        std::string name;
        if (lookup(cache, key, now, name))
            EXPECT_EQ(name, nameFor(key));
    }
}
