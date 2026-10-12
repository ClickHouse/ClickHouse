/// Runs identical randomized insert/remove/search/iterate sequences on `CuckooHash` and on jemalloc's `ckh`
/// (called through `cuckoo_hash_oracle_ref.c` with a real tsd of the reference jemalloc) and compares, after every
/// step, the results, the table geometry, the PRNG state, the table size and the cell occupied by every key.

#include <allocator/CuckooHash.h>
#include <allocator/ThreadState.h>

#include "Test.h"

#include <cstdlib>
#include <cstring>
#include <random>

extern "C" {
int ref_cuckoo_hash_new(size_t min_items, void (*hash)(const void *, size_t[2]), bool (*key_compare)(const void *, const void *));
void ref_cuckoo_hash_delete();
int ref_cuckoo_hash_insert(const void * key, const void * data);
int ref_cuckoo_hash_remove(const void * search_key, void ** key, void ** data);
int ref_cuckoo_hash_search(const void * search_key, void ** key, void ** data);
int ref_cuckoo_hash_iterate(size_t * table_idx, void ** key, void ** data);
size_t ref_cuckoo_hash_count();
unsigned ref_cuckoo_hash_log2_min_buckets();
unsigned ref_cuckoo_hash_log2_current_buckets();
uint64_t ref_cuckoo_hash_prng_state();
const void * ref_cuckoo_hash_cell_key(size_t i);
const void * ref_cuckoo_hash_cell_data(size_t i);
size_t ref_cuckoo_hash_table_usable_size();
size_t ref_sizeof_cuckoo_hash();
unsigned ref_log2_cuckoo_hash_bucket_cells();

/// jemalloc's hash functions (`ckh.c`, exported unprefixed from the reference library).
void ckh_string_hash(const void * key, size_t result_hash[2]);
bool ckh_string_keycomp(const void * k1, const void * k2);
void ckh_pointer_hash(const void * key, size_t result_hash[2]);
bool ckh_pointer_keycomp(const void * k1, const void * k2);

/// `ckh.o` pulls in the rest of the reference jemalloc, including the libunwind-based profiler backtrace, which is
/// never called here.
int unw_backtrace(void **, int)
{
    return 0;
}
}

using namespace jemalloc;

namespace
{

/// The table allocator of the test: system memory; remembers the usable size of the live tables.
struct TestAllocator
{
    static constexpr size_t MAX_TABLES = 8;
    static inline void * tables[MAX_TABLES] = {};
    static inline size_t usable_sizes[MAX_TABLES] = {};
    static inline size_t allocations = 0;
    static inline bool fail = false;

    static void * allocate(ThreadState &, size_t usable_size, size_t alignment)
    {
        CHECK_EQ(alignment, CACHE_LINE);
        if (fail)
            return nullptr;
        void * ptr = std::aligned_alloc(alignment, usable_size);
        REQUIRE(ptr != nullptr);
        std::memset(ptr, 0, usable_size);
        for (size_t i = 0; i < MAX_TABLES; ++i)
        {
            if (tables[i] == nullptr)
            {
                tables[i] = ptr;
                usable_sizes[i] = usable_size;
                ++allocations;
                return ptr;
            }
        }
        REQUIRE(false);
        return nullptr;
    }

    static void deallocate(ThreadState &, void * ptr)
    {
        for (size_t i = 0; i < MAX_TABLES; ++i)
        {
            if (tables[i] == ptr)
            {
                tables[i] = nullptr;
                std::free(ptr);
                return;
            }
        }
        REQUIRE(false);
    }

    static size_t usableSizeOf(const void * ptr)
    {
        for (size_t i = 0; i < MAX_TABLES; ++i)
            if (tables[i] == ptr)
                return usable_sizes[i];
        REQUIRE(false);
        return 0;
    }

    static size_t live()
    {
        size_t n = 0;
        for (auto * table : tables)
            n += table != nullptr;
        return n;
    }
};

using Table = CuckooHash<TestAllocator>;

ThreadState test_thread_state;

/// A deliberately weak hash with only 5 significant bits per word, so that small tables have many collisions, long
/// eviction chains, cycles and failed rebuilds, and some keys have both hashes in the same bucket.
void weakHash(const void * key, size_t result_hash[2])
{
    uint64_t k = reinterpret_cast<uintptr_t>(key) >> 4;
    uint64_t h = k * 0x9E3779B97F4A7C15ULL;
    result_hash[0] = (h >> 40) & 31;
    result_hash[1] = (k % 5 == 0) ? result_hash[0] : ((h >> 20) & 31);
}

bool weakKeyCompare(const void * k1, const void * k2)
{
    return k1 == k2;
}

void compareState(const Table & table, const char * what, size_t step)
{
    bool ok = true;
    if (table.count() != ref_cuckoo_hash_count() || table.log2CurrentBuckets() != ref_cuckoo_hash_log2_current_buckets()
        || table.log2MinBuckets() != ref_cuckoo_hash_log2_min_buckets() || table.prngState() != ref_cuckoo_hash_prng_state()
        || TestAllocator::usableSizeOf(table.cells()) != ref_cuckoo_hash_table_usable_size())
    {
        ok = false;
    }
    else
    {
        for (size_t i = 0; i < table.numCells(); ++i)
        {
            if (table.cells()[i].key != ref_cuckoo_hash_cell_key(i) || table.cells()[i].data != ref_cuckoo_hash_cell_data(i))
            {
                ok = false;
                break;
            }
        }
    }
    if (!ok)
    {
        std::fprintf(
            stderr,
            "%s: state differs after step %zu (count %zu/%zu, lg_cur %u/%u, prng %llu/%llu, usize %zu/%zu)\n",
            what,
            step,
            table.count(),
            ref_cuckoo_hash_count(),
            table.log2CurrentBuckets(),
            ref_cuckoo_hash_log2_current_buckets(),
            static_cast<unsigned long long>(table.prngState()),
            static_cast<unsigned long long>(ref_cuckoo_hash_prng_state()),
            TestAllocator::usableSizeOf(table.cells()),
            ref_cuckoo_hash_table_usable_size());
        CHECK(false);
        allocator_test::abortTest();
    }

    /// Iteration order (implied by the layout, but this is the interface the profiler uses).
    size_t idx = 0;
    size_t ref_idx = 0;
    size_t n = 0;
    while (true)
    {
        void * key = nullptr;
        void * data = nullptr;
        void * ref_key = nullptr;
        void * ref_data = nullptr;
        bool end = table.iterate(&idx, &key, &data);
        bool ref_end = ref_cuckoo_hash_iterate(&ref_idx, &ref_key, &ref_data) != 0;
        REQUIRE(end == ref_end);
        if (end)
            break;
        REQUIRE(idx == ref_idx);
        REQUIRE(key == ref_key);
        REQUIRE(data == ref_data);
        ++n;
    }
    REQUIRE(n == table.count());
}

struct ScenarioStats
{
    size_t grows = 0;
    size_t multi_grows = 0;
    size_t failed_rebuilds = 0;
    size_t shrinks = 0;
};

/// Runs `steps` random operations on a pool of `nkeys` keys produced by `makeKey`.
template <typename MakeKey>
ScenarioStats runScenario(
    const char * what,
    size_t min_items,
    CuckooHashFunction hash,
    CuckooKeyCompare key_compare,
    CuckooHashFunction ref_hash,
    CuckooKeyCompare ref_key_compare,
    size_t num_keys,
    size_t steps,
    uint64_t seed,
    MakeKey && makeKey)
{
    Table table;
    REQUIRE(!table.init(test_thread_state, min_items, hash, key_compare));
    REQUIRE(ref_cuckoo_hash_new(min_items, ref_hash, ref_key_compare) == 0);
    compareState(table, what, 0);

    constexpr size_t MAX_KEYS = 4096;
    REQUIRE(num_keys <= MAX_KEYS);
    static bool present[MAX_KEYS];
    std::memset(present, 0, sizeof(present));

    ScenarioStats stats;
    std::mt19937_64 rng(seed);
    /// Phases: mostly inserting, then mixed, then mostly removing (exercises growing and shrinking).
    for (size_t step = 1; step <= steps; ++step)
    {
        size_t phase = step * 3 / (steps + 1);
        unsigned insert_percent = phase == 0 ? 80 : (phase == 1 ? 50 : 15);
        size_t k = rng() % num_keys;
        const void * key = makeKey(k);
        const void * data = reinterpret_cast<const void *>(uintptr_t(rng() | 1));
        unsigned op = unsigned(rng() % 100);

        if (op < insert_percent)
        {
            if (!present[k])
            {
                unsigned log2_before = table.log2CurrentBuckets();
                size_t allocations_before = TestAllocator::allocations;
                bool error = table.insert(test_thread_state, key, data);
                int ref_error = ref_cuckoo_hash_insert(key, data);
                REQUIRE(error == (ref_error != 0));
                REQUIRE(!error);
                present[k] = true;
                if (table.log2CurrentBuckets() > log2_before + 1)
                    ++stats.multi_grows;
                if (TestAllocator::allocations > allocations_before + 1)
                    ++stats.failed_rebuilds;
                if (table.log2CurrentBuckets() > log2_before)
                    ++stats.grows;
            }
        }
        else if (op < 95)
        {
            unsigned log2_before = table.log2CurrentBuckets();
            void * removed_key = nullptr;
            void * removed_data = nullptr;
            void * ref_removed_key = nullptr;
            void * ref_removed_data = nullptr;
            bool not_found = table.remove(test_thread_state, key, &removed_key, &removed_data);
            bool ref_not_found = ref_cuckoo_hash_remove(key, &ref_removed_key, &ref_removed_data) != 0;
            REQUIRE(not_found == ref_not_found);
            REQUIRE(not_found == !present[k]);
            CHECK(removed_key == ref_removed_key);
            CHECK(removed_data == ref_removed_data);
            present[k] = false;
            if (table.log2CurrentBuckets() < log2_before)
                ++stats.shrinks;
        }
        else
        {
            void * found_key = nullptr;
            void * found_data = nullptr;
            void * ref_found_key = nullptr;
            void * ref_found_data = nullptr;
            bool not_found = table.search(key, &found_key, &found_data);
            bool ref_not_found = ref_cuckoo_hash_search(key, &ref_found_key, &ref_found_data) != 0;
            REQUIRE(not_found == ref_not_found);
            REQUIRE(not_found == !present[k]);
            CHECK(found_key == ref_found_key);
            CHECK(found_data == ref_found_data);
            /// Null output pointers are allowed.
            CHECK_EQ(table.search(key, nullptr, nullptr), not_found);
        }
        compareState(table, what, step);
    }

    table.destroy(test_thread_state);
    ref_cuckoo_hash_delete();
    CHECK_EQ(TestAllocator::live(), size_t(0));
    return stats;
}

const void * pointerKey(size_t k)
{
    return reinterpret_cast<const void *>(uintptr_t(0x10000) + k * 16);
}

/// Decimal strings (own static storage).
const void * stringKey(size_t k)
{
    static char storage[4096][16];
    std::snprintf(storage[k], sizeof(storage[k]), "key-%zu", k * 7919);
    return storage[k];
}

}

TEST(CuckooHashOracle, Layout)
{
    CHECK_EQ(sizeof(CuckooHashBase), ref_sizeof_cuckoo_hash());
    CHECK_EQ(sizeof(Table), ref_sizeof_cuckoo_hash());
    CHECK_EQ(LOG2_CUCKOO_HASH_BUCKET_CELLS, ref_log2_cuckoo_hash_bucket_cells());
}

TEST(CuckooHashOracle, HashFunctions)
{
    for (size_t k = 0; k < 1000; ++k)
    {
        size_t expected[2];
        size_t actual[2];
        ckh_pointer_hash(pointerKey(k), expected);
        cuckooHashPointerHash(pointerKey(k), actual);
        CHECK_EQ(actual[0], expected[0]);
        CHECK_EQ(actual[1], expected[1]);
        ckh_string_hash(stringKey(k), expected);
        cuckooHashStringHash(stringKey(k), actual);
        CHECK_EQ(actual[0], expected[0]);
        CHECK_EQ(actual[1], expected[1]);
        CHECK_EQ(cuckooHashStringKeyCompare(stringKey(k), stringKey(k)), ckh_string_keycomp(stringKey(k), stringKey(k)));
        CHECK_EQ(cuckooHashStringKeyCompare(stringKey(k), stringKey(k + 1)), ckh_string_keycomp(stringKey(k), stringKey(k + 1)));
        CHECK_EQ(cuckooHashPointerKeyCompare(pointerKey(k), pointerKey(k + 1)), ckh_pointer_keycomp(pointerKey(k), pointerKey(k + 1)));
    }
}

TEST(CuckooHashOracle, InitialGeometry)
{
    /// Every `min_items` up to a few thousand: the same table size as jemalloc (e.g. 64 -> 32 buckets, 2048 bytes).
    for (size_t min_items = 1; min_items <= 5000; min_items += (min_items < 300 ? 1 : 97))
    {
        Table table;
        REQUIRE(!table.init(test_thread_state, min_items, cuckooHashPointerHash, cuckooHashPointerKeyCompare));
        REQUIRE(ref_cuckoo_hash_new(min_items, ckh_pointer_hash, ckh_pointer_keycomp) == 0);
        compareState(table, "geometry", min_items);
        if (min_items == 64)
        {
            CHECK_EQ(table.log2CurrentBuckets(), 5u);
            CHECK_EQ(TestAllocator::usableSizeOf(table.cells()), size_t(2048));
        }
        table.destroy(test_thread_state);
        ref_cuckoo_hash_delete();
    }
}

TEST(CuckooHashOracle, PointerKeys)
{
    runScenario(
        "pointer/64",
        64,
        cuckooHashPointerHash,
        cuckooHashPointerKeyCompare,
        ckh_pointer_hash,
        ckh_pointer_keycomp,
        600,
        6000,
        1,
        pointerKey);
    runScenario(
        "pointer/1",
        1,
        cuckooHashPointerHash,
        cuckooHashPointerKeyCompare,
        ckh_pointer_hash,
        ckh_pointer_keycomp,
        2000,
        12000,
        2,
        pointerKey);
    runScenario(
        "pointer/7", 7, cuckooHashPointerHash, cuckooHashPointerKeyCompare, ckh_pointer_hash, ckh_pointer_keycomp, 50, 3000, 3, pointerKey);
}

TEST(CuckooHashOracle, StringKeys)
{
    runScenario(
        "string/64", 64, cuckooHashStringHash, cuckooHashStringKeyCompare, ckh_string_hash, ckh_string_keycomp, 500, 5000, 4, stringKey);
    runScenario(
        "string/2", 2, cuckooHashStringHash, cuckooHashStringKeyCompare, ckh_string_hash, ckh_string_keycomp, 1500, 9000, 5, stringKey);
}

TEST(CuckooHashOracle, WeakHashEvictionsAndRebuildFailures)
{
    /// At most 32 buckets (128 cells) are reachable, so the tables grow while failing rebuilds, and evictions form cycles.
    ScenarioStats total;
    for (uint64_t seed = 10; seed < 16; ++seed)
    {
        ScenarioStats stats
            = runScenario("weak", 1 + seed % 4, weakHash, weakKeyCompare, weakHash, weakKeyCompare, 110, 4000, seed, pointerKey);
        total.grows += stats.grows;
        total.multi_grows += stats.multi_grows;
        total.failed_rebuilds += stats.failed_rebuilds;
        total.shrinks += stats.shrinks;
    }
    std::fprintf(
        stderr,
        "weak hash: %zu grows, %zu multi-step grows, %zu failed rebuilds, %zu shrinks\n",
        total.grows,
        total.multi_grows,
        total.failed_rebuilds,
        total.shrinks);
    /// The scenario must exercise the rare paths.
    CHECK_GT(total.multi_grows, size_t(0));
    CHECK_GT(total.failed_rebuilds, size_t(0));
    CHECK_GT(total.shrinks, size_t(0));
}

TEST(CuckooHashOracle, AllocationFailure)
{
    /// When the allocator fails during a grow, `insert` returns true and keeps the old table (in which the item left
    /// over from the eviction chain is missing, as in jemalloc). Not compared with the reference, since its
    /// allocation cannot be made to fail.
    Table table;
    REQUIRE(!table.init(test_thread_state, 1, cuckooHashPointerHash, cuckooHashPointerKeyCompare));
    TestAllocator::fail = true;
    bool failed = false;
    size_t k = 0;
    for (; k < 100 && !failed; ++k)
        failed = table.insert(test_thread_state, pointerKey(k), pointerKey(k));
    TestAllocator::fail = false;
    CHECK(failed);
    CHECK_EQ(table.log2CurrentBuckets(), table.log2MinBuckets());
    size_t occupied = 0;
    for (size_t i = 0; i < table.numCells(); ++i)
        occupied += table.cells()[i].key != nullptr;
    CHECK_EQ(table.count(), occupied);
    CHECK_EQ(table.count(), k - 1);
    table.destroy(test_thread_state);

    /// `init` fails if the table cannot be allocated.
    TestAllocator::fail = true;
    Table table2;
    CHECK(table2.init(test_thread_state, 64, cuckooHashPointerHash, cuckooHashPointerKeyCompare));
    TestAllocator::fail = false;
}
