/// Unit tests of the `mallctl` machinery (`Mallctl.h`): name and numeric path lookup, partial names and numeric paths, error codes of the
/// access checks, the leaves that are implemented without other subsystems. The numeric paths of all names are compared with
/// the reference jemalloc in mallctl_names_oracle.cpp; the values pinned here are jemalloc's.

#include <allocator/Arenas.h>
#include <allocator/BackgroundThread.h>
#include <allocator/Base.h>
#include <allocator/ExtentMap.h>
#include <allocator/Mallctl.h>
#include <allocator/MallctlImpl.h>
#include <allocator/Options.h>
#include <allocator/Pages.h>
#include <allocator/SizeClasses.h>
#include <allocator/ThreadCache.h>
#include <allocator/ThreadState.h>

#include "Test.h"

#include <atomic>

#include <cerrno>
#include <cstring>

using namespace jemalloc;

namespace
{

ThreadState test_thread_state;

constexpr unsigned TEST_NUM_ARENAS = 4;

/// Boots the arena layer as the a0 initialization does (`arena_boot`, the global extent map, the arena table) and
/// creates real arenas (the ctl merges their stats on refresh).
void bootArenas()
{
    sizeBoot(default_size_class_data, options.cache_oblivious);
    REQUIRE(!arena_extent_map_global.init(base0Get(), /* zeroed */ true));
    REQUIRE(!arenaBoot(&default_size_class_data, base0Get(), false));
    REQUIRE(!arenas_lock.init("arenas", MutexRank::ARENAS, MutexLockOrder::RankExclusive));
    num_arenas_auto = 1;
    manual_arena_base = 2;
    numArenasTotalSet(1);
    REQUIRE(!backgroundThreadBoot0());
    REQUIRE(!backgroundThreadBoot1(nullptr, base0Get()));
}

/// `arenaNew` enters and leaves reentrancy for `idx != 0`, which needs a tsd in a nominal state.
ThreadState & arenaThreadState()
{
    static ThreadState thread_state;
    thread_state.state.store(thread_state_nominal_slow, std::memory_order_relaxed);
    return thread_state;
}

/// Arenas 0 and 1 exist (of `TEST_NUM_ARENAS`).
void setUpArenaTable()
{
    bootArenas();
    for (unsigned i = 0; i < 2; ++i)
        REQUIRE(arenaNew(&arenaThreadState(), i, &arena_config_default) != nullptr);
    numArenasTotalSet(TEST_NUM_ARENAS);
}

void setUp()
{
    static bool done = false;
    if (done)
        return;
    done = true;
    REQUIRE(!pages::boot());
    REQUIRE(!baseBoot(nullptr));
    setUpArenaTable();
    REQUIRE(!mallctlBoot());
    /// The mallctl tsd is nominal (slow): `arenas.create` creates arenas, which enters reentrancy.
    test_thread_state.state.store(thread_state_nominal_slow, std::memory_order_relaxed);
    test_thread_state.radix_tree_context.init();
}

int nameToNumericPath(const char * name, size_t * numeric_path, size_t * numeric_path_length)
{
    setUp();
    return mallctlNameToNumericPath(test_thread_state, name, numeric_path, numeric_path_length);
}

/// Returns the error code; `*numeric_path`/`*len` are filled on success.
int lookup(const char * name, size_t (&numeric_path)[MALLCTL_MAX_DEPTH], size_t & len)
{
    len = MALLCTL_MAX_DEPTH;
    std::memset(numeric_path, 0xff, sizeof(numeric_path));
    return nameToNumericPath(name, numeric_path, &len);
}

int byName(const char * name, void * old_value, size_t * old_length_ptr, void * new_value = nullptr, size_t new_length = 0)
{
    setUp();
    return mallctlByName(test_thread_state, name, old_value, old_length_ptr, new_value, new_length);
}

template <typename T>
int readValue(const char * name, T & value)
{
    size_t len = sizeof(T);
    int result = byName(name, &value, &len);
    if (result == 0)
        CHECK_EQ(len, sizeof(T));
    return result;
}

void checkNumericPath(const char * name, std::initializer_list<size_t> expected)
{
    size_t numeric_path[MALLCTL_MAX_DEPTH];
    size_t len;
    int result = lookup(name, numeric_path, len);
    CHECK_EQ(result, 0);
    CHECK_EQ(len, expected.size());
    size_t i = 0;
    for (size_t e : expected)
    {
        if (i < len && numeric_path[i] != e)
        {
            std::fprintf(stderr, "%s: numeric_path[%zu] = %zu, expected %zu\n", name, i, numeric_path[i], e);
            CHECK(false);
        }
        ++i;
    }
}

void checkNoEntry(const char * name)
{
    size_t numeric_path[MALLCTL_MAX_DEPTH];
    size_t len;
    int result = lookup(name, numeric_path, len);
    if (result != ENOENT)
    {
        std::fprintf(stderr, "\"%s\": expected ENOENT, got %d\n", name, result);
        CHECK(false);
    }
}

}

TEST(Mallctl, NameToNumericPath)
{
    checkNumericPath("version", {0});
    checkNumericPath("epoch", {1});
    checkNumericPath("thread.tcache.ncached_max.write", {4, 5, 3, 1});
    checkNumericPath("config.xmalloc", {5, 12});
    checkNumericPath("opt.abort", {6, 0});
    checkNumericPath("opt.lg_extent_max_active_fit", {6, 55});
    checkNumericPath("opt.malloc_conf.global_var_2_conf_harder", {6, 78, 3});
    checkNumericPath("arena.0.decay", {8, 0, 1});
    checkNumericPath("arena.4096.name", {8, 4096, 11});
    checkNumericPath("arenas.bin.3.nshards", {9, 9, 3, 3});
    checkNumericPath("arenas.lookup", {9, 13});
    checkNumericPath("prof.stats", {10, 10});
    checkNumericPath("stats.arenas.0.bins.5.mutex.max_num_thds", {11, 11, 0, 34, 5, 10, 6});
    checkNumericPath("stats.arenas.4096.hpa_shard.nonfull_slabs.63.ndirty_huge", {11, 11, 4096, 38, 11, 63, 5});
    checkNumericPath("stats.arenas.1.hpa_sec_dalloc_noflush", {11, 11, 1, 29});
    checkNumericPath("stats.mutexes.reset", {11, 10, 9});
    checkNumericPath("stats.zero_reallocs", {11, 12});
    checkNumericPath("approximate_stats.active", {12, 0});
    checkNumericPath("experimental.thread.activity_callback", {13, 6, 0});
}

TEST(Mallctl, PartialNames)
{
    /// Partial names succeed in `mallctlnametomib`.
    checkNumericPath("stats", {11});
    checkNumericPath("stats.arenas", {11, 11});
    checkNumericPath("stats.arenas.0", {11, 11, 0});
    checkNumericPath("arena.4097", {8, 4097});
    checkNumericPath("opt.malloc_conf", {6, 78});

    /// A too-small numeric path buffer returns a truncated prefix successfully.
    size_t numeric_path[MALLCTL_MAX_DEPTH] = {};
    size_t len = 2;
    CHECK_EQ(nameToNumericPath("stats.arenas.0.pactive", numeric_path, &len), 0);
    CHECK_EQ(len, size_t(2));
    CHECK_EQ(numeric_path[0], size_t(11));
    CHECK_EQ(numeric_path[1], size_t(11));
    CHECK_EQ(numeric_path[2], size_t(0));

    /// ... but they are not leaves.
    size_t value_len = sizeof(size_t);
    size_t value = 0;
    CHECK_EQ(byName("stats", &value, &value_len), ENOENT);
    CHECK_EQ(byName("stats.arenas.0", &value, &value_len), ENOENT);
}

TEST(Mallctl, InvalidNames)
{
    checkNoEntry("");
    checkNoEntry(".");
    checkNoEntry(".version");
    checkNoEntry("version.");
    checkNoEntry("version.x");
    checkNoEntry("versio");
    checkNoEntry("versionx");
    checkNoEntry("stats..arenas");
    checkNoEntry("stats.");
    checkNoEntry("stats.arenas.");
    checkNoEntry("stats.arenas.0.");
    checkNoEntry("stats.arenas.x");
    checkNoEntry("stats.arenas.-1");
    checkNoEntry("arena.18446744073709551615");
    checkNoEntry("arena.18446744073709551614");
    checkNoEntry("arena.99999999999999999999.decay");
    /// Arena indices: `i <= num_arenas` (`num_arenas` is the alias of MALLCTL_ARENAS_ALL), 4096, 4097.
    checkNumericPath("arena.4", {8, 4});
    checkNoEntry("arena.5");
    checkNoEntry("arena.4095");
    checkNoEntry("arena.4098");
    /// `stats.arenas.<i>`: only initialized arenas (at the last refresh), the alias, and 4096 (4097 only after a
    /// destroy).
    checkNumericPath("stats.arenas.1", {11, 11, 1});
    checkNoEntry("stats.arenas.2");
    checkNumericPath("stats.arenas.4", {11, 11, 4});
    checkNumericPath("stats.arenas.4096", {11, 11, 4096});
    checkNoEntry("stats.arenas.4097");
    checkNoEntry("experimental.arenas.4097");
    checkNumericPath("experimental.arenas.0.pactivep", {13, 2, 0, 0});
    /// `prof.stats.*` only exist with `profiling` and `profiling_stats`.
    checkNoEntry("prof.stats.bins.0");
    checkNumericPath("prof.stats.bins", {10, 10, 0});
}

TEST(Mallctl, LenientIndices)
{
    /// `malloc_strtoumax` skips whitespace, accepts a sign, and stops at the first non-digit (the element is still
    /// delimited by the dot).
    checkNumericPath("stats.arenas.0x.pactive", {11, 11, 0, 5});
    checkNumericPath("stats.arenas. 1.pactive", {11, 11, 1, 5});
    checkNumericPath("stats.arenas.+1.pactive", {11, 11, 1, 5});
    checkNumericPath("arenas.bin.007.size", {9, 9, 7, 0});
    checkNoEntry("stats.arenas..pactive");
}

TEST(Mallctl, OffByOneIndices)
{
    /// jemalloc accepts `SC_NBINS` and `SIZE_CLASS_NUM_SIZES - SIZE_CLASS_NUM_BINS` (off by one) and rejects the next one.
    char name[64];
    std::snprintf(name, sizeof(name), "arenas.bin.%u.size", SIZE_CLASS_NUM_BINS);
    checkNumericPath(name, {9, 9, SIZE_CLASS_NUM_BINS, 0});
    std::snprintf(name, sizeof(name), "arenas.bin.%u.size", SIZE_CLASS_NUM_BINS + 1);
    checkNoEntry(name);
    std::snprintf(name, sizeof(name), "arenas.lextent.%u.size", SIZE_CLASS_NUM_SIZES - SIZE_CLASS_NUM_BINS);
    checkNumericPath(name, {9, 11, SIZE_CLASS_NUM_SIZES - SIZE_CLASS_NUM_BINS, 0});
    std::snprintf(name, sizeof(name), "arenas.lextent.%u.size", SIZE_CLASS_NUM_SIZES - SIZE_CLASS_NUM_BINS + 1);
    checkNoEntry(name);
    std::snprintf(name, sizeof(name), "stats.arenas.0.bins.%u.nmalloc", SIZE_CLASS_NUM_BINS);
    checkNumericPath(name, {11, 11, 0, 34, SIZE_CLASS_NUM_BINS, 0});
    std::snprintf(name, sizeof(name), "stats.arenas.0.extents.%u.ndirty", SIZE_CLASS_NUM_PAGE_SIZES - 1);
    checkNumericPath(name, {11, 11, 0, 36, SIZE_CLASS_NUM_PAGE_SIZES - 1, 0});
    std::snprintf(name, sizeof(name), "stats.arenas.0.extents.%u.ndirty", SIZE_CLASS_NUM_PAGE_SIZES);
    checkNoEntry(name);
    checkNoEntry("stats.arenas.0.hpa_shard.nonfull_slabs.64");

    /// The past-the-end entries read as zeros.
    size_t size = 1;
    std::snprintf(name, sizeof(name), "arenas.bin.%u.size", SIZE_CLASS_NUM_BINS);
    CHECK_EQ(readValue(name, size), 0);
    CHECK_EQ(size, size_t(0));
    size = 1;
    std::snprintf(name, sizeof(name), "arenas.lextent.%u.size", SIZE_CLASS_NUM_SIZES - SIZE_CLASS_NUM_BINS);
    CHECK_EQ(readValue(name, size), 0);
    CHECK_EQ(size, size_t(0));
}

TEST(Mallctl, ByNumericPath)
{
    setUp();
    size_t numeric_path[MALLCTL_MAX_DEPTH];
    size_t len;
    REQUIRE(lookup("arenas.page", numeric_path, len) == 0);
    size_t page = 0;
    size_t page_len = sizeof(page);
    CHECK_EQ(mallctlByNumericPath(test_thread_state, numeric_path, len, &page, &page_len, nullptr, 0), 0);
    CHECK_EQ(page, PAGE);

    /// Partial numeric path.
    CHECK_EQ(mallctlByNumericPath(test_thread_state, numeric_path, 1, &page, &page_len, nullptr, 0), ENOENT);
    /// Out of range.
    size_t bad[] = {9, 14};
    CHECK_EQ(mallctlByNumericPath(test_thread_state, bad, 2, &page, &page_len, nullptr, 0), ENOENT);
    size_t bad_root[] = {14};
    CHECK_EQ(mallctlByNumericPath(test_thread_state, bad_root, 1, &page, &page_len, nullptr, 0), ENOENT);
    /// Longer than the path.
    size_t longer[] = {9, 4, 0};
    CHECK_EQ(mallctlByNumericPath(test_thread_state, longer, 3, &page, &page_len, nullptr, 0), ENOENT);
    /// Invalid index.
    size_t bad_index[] = {8, 4095, 0};
    CHECK_EQ(mallctlByNumericPath(test_thread_state, bad_index, 3, &page, &page_len, nullptr, 0), ENOENT);
    /// An empty numeric path is the root.
    CHECK_EQ(mallctlByNumericPath(test_thread_state, numeric_path, 0, &page, &page_len, nullptr, 0), ENOENT);

    /// The numeric path of `arenas.bin.<i>.size` with every bin.
    REQUIRE(lookup("arenas.bin.0.size", numeric_path, len) == 0);
    for (unsigned i = 0; i < SIZE_CLASS_NUM_BINS; ++i)
    {
        numeric_path[2] = i;
        size_t size = 0;
        size_t size_len = sizeof(size);
        CHECK_EQ(mallctlByNumericPath(test_thread_state, numeric_path, len, &size, &size_len, nullptr, 0), 0);
        CHECK_EQ(size, size_classes::indexToSize(i));
    }
}

TEST(Mallctl, ExtendNumericPathByName)
{
    setUp();
    size_t numeric_path[MALLCTL_MAX_DEPTH];
    size_t len;
    REQUIRE(lookup("stats.arenas", numeric_path, len) == 0);
    REQUIRE(len == 2);

    size_t total = MALLCTL_MAX_DEPTH;
    CHECK_EQ(mallctlExtendNumericPathByName(test_thread_state, numeric_path, 2, "0.bins.3.curregs", &total), 0);
    CHECK_EQ(total, size_t(6));
    CHECK_EQ(numeric_path[2], size_t(0));
    CHECK_EQ(numeric_path[3], size_t(34));
    CHECK_EQ(numeric_path[4], size_t(3));
    CHECK_EQ(numeric_path[5], size_t(3));

    /// The starting node must not be a leaf.
    size_t leaf_numeric_path[MALLCTL_MAX_DEPTH];
    REQUIRE(lookup("version", leaf_numeric_path, len) == 0);
    total = MALLCTL_MAX_DEPTH;
    CHECK_EQ(mallctlExtendNumericPathByName(test_thread_state, leaf_numeric_path, 1, "x", &total), ENOENT);
    /// Unknown name relative to the node.
    total = MALLCTL_MAX_DEPTH;
    CHECK_EQ(mallctlExtendNumericPathByName(test_thread_state, numeric_path, 2, "0.no_such", &total), ENOENT);

    /// `mallctlByNumericPathAndName` reads the leaf (here relative to the root).
    total = MALLCTL_MAX_DEPTH;
    const char * version = nullptr;
    size_t version_len = sizeof(version);
    CHECK_EQ(mallctlByNumericPathAndName(test_thread_state, numeric_path, 0, "version", &total, &version, &version_len, nullptr, 0), 0);
    CHECK_EQ(total, size_t(1));
    CHECK_STREQ(version, "5.3-RC");
    /// A partial name relative to a node is not a leaf.
    REQUIRE(lookup("stats.arenas", numeric_path, len) == 0);
    total = MALLCTL_MAX_DEPTH;
    CHECK_EQ(mallctlByNumericPathAndName(test_thread_state, numeric_path, 2, "0", &total, &version, &version_len, nullptr, 0), ENOENT);
}

TEST(Mallctl, ReadWriteChecks)
{
    /// READ: a size mismatch copies a partial value and returns EINVAL.
    const char * version = nullptr;
    size_t len = sizeof(version);
    CHECK_EQ(byName("version", &version, &len), 0);
    CHECK_STREQ(version, "5.3-RC");

    uint64_t epoch = 0;
    len = sizeof(epoch);
    CHECK_EQ(byName("epoch", &epoch, &len), 0);
    /// The first mallctl initializes the ctl, which refreshes once.
    CHECK_EQ(epoch, uint64_t(1));

    uint32_t small = 0xdeadbeef;
    len = sizeof(small);
    CHECK_EQ(byName("epoch", &small, &len), EINVAL);
    CHECK_EQ(len, sizeof(small));
    /// A partial copy of the first bytes of the 64-bit value.
    CHECK_EQ(small, config::big_endian ? uint32_t(epoch >> 32) : uint32_t(epoch));

    unsigned char big[16];
    std::memset(big, 0xaa, sizeof(big));
    len = sizeof(big);
    CHECK_EQ(byName("epoch", big, &len), EINVAL);
    CHECK_EQ(len, sizeof(uint64_t));
    uint64_t copied;
    std::memcpy(&copied, big, sizeof(copied));
    CHECK_EQ(copied, epoch);
    CHECK_EQ(big[8], 0xaa);

    /// No read if `old_value` or `old_length_ptr` is null.
    CHECK_EQ(byName("epoch", nullptr, &len), 0);
    CHECK_EQ(byName("epoch", &epoch, nullptr), 0);

    /// WRITE: a wrong size is EINVAL; a write refreshes and the read returns the new epoch.
    uint64_t any = 12345;
    CHECK_EQ(byName("epoch", nullptr, nullptr, &any, 4), EINVAL);
    len = sizeof(epoch);
    CHECK_EQ(byName("epoch", &epoch, &len, &any, sizeof(any)), 0);
    CHECK_EQ(epoch, uint64_t(2));
    /// `new_value == nullptr` with `new_length != 0` passes WRITE (and does not refresh).
    CHECK_EQ(byName("epoch", &epoch, &len, nullptr, 8), 0);
    CHECK_EQ(epoch, uint64_t(2));

    /// READONLY.
    CHECK_EQ(byName("version", nullptr, nullptr, &version, sizeof(version)), EPERM);
    CHECK_EQ(byName("version", nullptr, nullptr, nullptr, 1), EPERM);
    CHECK_EQ(byName("arenas.page", nullptr, nullptr, &any, sizeof(size_t)), EPERM);
    CHECK_EQ(byName("opt.abort", nullptr, nullptr, nullptr, 1), EPERM);
    CHECK_EQ(byName("stats.allocated", nullptr, nullptr, nullptr, 1), EPERM);
    /// READONLY is checked before MIB_UNSIGNED.
    CHECK_EQ(byName("arena.0.initialized", nullptr, nullptr, nullptr, 1), EPERM);
}

TEST(Mallctl, AccessCheckHelpers)
{
    int x = 0;
    size_t len = 0;
    CHECK_EQ(mallctl::readOnly(nullptr, 0), 0);
    CHECK_EQ(mallctl::readOnly(&x, 0), EPERM);
    CHECK_EQ(mallctl::readOnly(nullptr, 1), EPERM);
    CHECK_EQ(mallctl::writeOnly(nullptr, nullptr), 0);
    CHECK_EQ(mallctl::writeOnly(&x, nullptr), EPERM);
    CHECK_EQ(mallctl::writeOnly(nullptr, &len), EPERM);
    CHECK_EQ(mallctl::readXorWrite(&x, &len, nullptr, 0), 0);
    CHECK_EQ(mallctl::readXorWrite(nullptr, nullptr, &x, 4), 0);
    CHECK_EQ(mallctl::readXorWrite(&x, nullptr, &x, 4), 0);
    CHECK_EQ(mallctl::readXorWrite(&x, &len, &x, 4), EPERM);
    CHECK_EQ(mallctl::readXorWrite(&x, &len, nullptr, 4), EPERM);
    CHECK_EQ(mallctl::neitherReadNorWrite(nullptr, nullptr, nullptr, 0), 0);
    CHECK_EQ(mallctl::neitherReadNorWrite(nullptr, &len, nullptr, 0), EPERM);
    CHECK_EQ(mallctl::neitherReadNorWrite(nullptr, nullptr, nullptr, 1), EPERM);

    /// VERIFY_READ zeroes `*old_length_ptr` on failure.
    len = 3;
    CHECK_EQ(mallctl::verifyRead<unsigned>(&x, &len), EINVAL);
    CHECK_EQ(len, size_t(0));
    len = sizeof(unsigned);
    CHECK_EQ(mallctl::verifyRead<unsigned>(&x, &len), 0);
    CHECK_EQ(mallctl::verifyRead<unsigned>(nullptr, &len), EINVAL);
    CHECK_EQ(len, size_t(0));
    CHECK_EQ(mallctl::verifyRead<unsigned>(&x, nullptr), EINVAL);

    /// ASSURED_WRITE.
    int v = 0;
    int src = 7;
    CHECK_EQ(mallctl::assuredWrite(nullptr, sizeof(int), v), EINVAL);
    CHECK_EQ(mallctl::assuredWrite(&src, 3, v), EINVAL);
    CHECK_EQ(mallctl::assuredWrite(&src, sizeof(int), v), 0);
    CHECK_EQ(v, 7);

    /// MIB_UNSIGNED.
    size_t numeric_path[] = {0, size_t(UINT_MAX) + 1, UINT_MAX};
    unsigned u = 0;
    CHECK_EQ(mallctl::numericPathComponentUnsigned(numeric_path, 1, u), EFAULT);
    CHECK_EQ(mallctl::numericPathComponentUnsigned(numeric_path, 2, u), 0);
    CHECK_EQ(u, UINT_MAX);
}

TEST(Mallctl, ConstantLeaves)
{
    const char * str = nullptr;
    bool b = true;
    size_t z = 0;
    unsigned u = 0;
    uint32_t u32 = 0;

    CHECK_EQ(readValue("config.cache_oblivious", b), 0);
    CHECK_EQ(b, true);
    CHECK_EQ(readValue("config.debug", b), 0);
    CHECK_EQ(b, false);
    CHECK_EQ(readValue("config.fill", b), 0);
    CHECK_EQ(b, true);
    CHECK_EQ(readValue("config.prof", b), 0);
    CHECK_EQ(b, true);
    CHECK_EQ(readValue("config.prof_libunwind", b), 0);
    CHECK_EQ(b, true);
    CHECK_EQ(readValue("config.stats", b), 0);
    CHECK_EQ(b, true);
    /// `ALLOCATOR_MALLOC_CONF` is only defined for the library sources, so the value is not compared here.
    CHECK_EQ(readValue("config.malloc_conf", str), 0);
    CHECK(str != nullptr);

    CHECK_EQ(readValue("arenas.quantum", z), 0);
    CHECK_EQ(z, size_t(16));
    CHECK_EQ(readValue("arenas.page", z), 0);
    CHECK_EQ(z, PAGE);
    CHECK_EQ(readValue("arenas.hugepage", z), 0);
    CHECK_EQ(z, HUGE_PAGE);
    CHECK_EQ(readValue("arenas.nbins", u), 0);
    CHECK_EQ(u, SIZE_CLASS_NUM_BINS);
    CHECK_EQ(readValue("arenas.nlextents", u), 0);
    CHECK_EQ(u, SIZE_CLASS_NUM_SIZES - SIZE_CLASS_NUM_BINS);
    CHECK_EQ(readValue("arenas.bin.0.size", z), 0);
    CHECK_EQ(z, size_t(8));
    CHECK_EQ(readValue("arenas.bin.0.nregs", u32), 0);
    CHECK_EQ(u32, bin_infos[0].num_regions);
    CHECK_EQ(readValue("arenas.bin.0.slab_size", z), 0);
    CHECK_EQ(z, bin_infos[0].slab_size);
    CHECK_EQ(readValue("arenas.bin.0.nshards", u32), 0);
    CHECK_EQ(u32, 1u);
    CHECK_EQ(readValue("arenas.lextent.0.size", z), 0);
    CHECK_EQ(z, SIZE_CLASS_LARGE_MIN_CLASS);
    /// `num_regions` is a `uint32_t`: an 8-byte buffer gets 4 bytes and EINVAL.
    size_t wide = 0;
    size_t len = sizeof(wide);
    CHECK_EQ(byName("arenas.bin.0.nregs", &wide, &len), EINVAL);
    CHECK_EQ(len, sizeof(uint32_t));
}

TEST(Mallctl, OptionLeaves)
{
    bool b = true;
    const char * str = nullptr;
    size_t z = 0;
    int64_t i64 = 0;
    ssize_t ss = 0;

    CHECK_EQ(readValue("opt.abort", b), 0);
    CHECK_EQ(b, options.abort);
    CHECK_EQ(readValue("opt.junk", str), 0);
    CHECK_STREQ(str, options.junk);
    CHECK_EQ(readValue("opt.dss", str), 0);
    CHECK_STREQ(str, "secondary");
    CHECK_EQ(readValue("opt.metadata_thp", str), 0);
    CHECK_STREQ(str, "disabled");
    CHECK_EQ(readValue("opt.thp", str), 0);
    CHECK_STREQ(str, "default");
    CHECK_EQ(readValue("opt.percpu_arena", str), 0);
    CHECK_STREQ(str, per_cpu_arena_mode_names[unsigned(options.per_cpu_arena)]);
    CHECK_EQ(readValue("opt.zero_realloc", str), 0);
    CHECK_STREQ(str, zero_realloc_mode_names[unsigned(options.zero_realloc_action)]);
    CHECK_EQ(readValue("opt.prof_time_resolution", str), 0);
    CHECK_STREQ(str, "default");
    CHECK_EQ(readValue("opt.hpa_hugify_style", str), 0);
    CHECK_STREQ(str, "lazy");
    CHECK_EQ(readValue("opt.prof_prefix", str), 0);
    CHECK_STREQ(str, "jeprof");
    CHECK_EQ(readValue("opt.oversize_threshold", z), 0);
    CHECK_EQ(z, options.oversize_threshold);
    CHECK_EQ(readValue("opt.mutex_max_spin", i64), 0);
    CHECK_EQ(i64, int64_t(600));
    CHECK_EQ(readValue("opt.dirty_decay_ms", ss), 0);
    CHECK_EQ(ss, options.dirty_decay_ms);
    CHECK_EQ(readValue("opt.stats_print_opts", str), 0);
    CHECK_STREQ(str, "");

    /// Options of disabled build features do not exist.
    CHECK_EQ(readValue("opt.utrace", b), ENOENT);
    CHECK_EQ(readValue("opt.xmalloc", b), ENOENT);
    CHECK_EQ(readValue("opt.lg_san_uaf_align", ss), config::use_after_free_detection ? 0 : ENOENT);
    /// `opt.malloc_conf.*` exist only when the corresponding source is set.
    CHECK_EQ(readValue("opt.malloc_conf.symlink", str), ENOENT);
    CHECK_EQ(readValue("opt.malloc_conf.env_var", str), options.malloc_conf_env_variable ? 0 : ENOENT);
    /// The condition is checked before READONLY.
    CHECK_EQ(byName("opt.utrace", nullptr, nullptr, nullptr, 1), ENOENT);
}

TEST(Mallctl, ArenaSlots)
{
    unsigned num_arenas = 0;
    CHECK_EQ(readValue("arenas.narenas", num_arenas), 0);
    CHECK_EQ(num_arenas, TEST_NUM_ARENAS);

    bool initialized = false;
    CHECK_EQ(readValue("arena.0.initialized", initialized), 0);
    CHECK_EQ(initialized, true);
    CHECK_EQ(readValue("arena.1.initialized", initialized), 0);
    CHECK_EQ(initialized, true);
    CHECK_EQ(readValue("arena.2.initialized", initialized), 0);
    CHECK_EQ(initialized, false);
    /// The merged slot is always initialized; the destroyed one only after a destroy; `num_arenas` is the merged one.
    CHECK_EQ(readValue("arena.4096.initialized", initialized), 0);
    CHECK_EQ(initialized, true);
    CHECK_EQ(readValue("arena.4097.initialized", initialized), 0);
    CHECK_EQ(initialized, false);
    CHECK_EQ(readValue("arena.4.initialized", initialized), 0);
    CHECK_EQ(initialized, true);

    /// The cleared slot of the merged stats.
    const char * sbrk = nullptr;
    CHECK_EQ(readValue("stats.arenas.4096.dss", sbrk), 0);
    CHECK_STREQ(sbrk, "N/A");
    ssize_t decay = 0;
    CHECK_EQ(readValue("stats.arenas.4096.dirty_decay_ms", decay), 0);
    CHECK_EQ(decay, ssize_t(-1));
    unsigned num_threads = 1;
    CHECK_EQ(readValue("stats.arenas.0.nthreads", num_threads), 0);
    CHECK_EQ(num_threads, 0u);
    size_t allocated = 1;
    CHECK_EQ(readValue("stats.arenas.0.small.allocated", allocated), 0);
    CHECK_EQ(allocated, size_t(0));
}

TEST(Mallctl, StatsLeaves)
{
    uint64_t epoch = 0;
    size_t len = sizeof(epoch);
    CHECK_EQ(byName("epoch", &epoch, &len, &epoch, sizeof(epoch)), 0);

    /// The profiling data of `mallctl_mutex` is read during the refresh (while it is held, so it includes that lock).
    uint64_t num_ops = 0;
    CHECK_EQ(readValue("stats.mutexes.ctl.num_ops", num_ops), 0);
    CHECK_GT(num_ops, uint64_t(0));
    uint32_t max_num_threads = 1;
    CHECK_EQ(readValue("stats.mutexes.ctl.max_num_thds", max_num_threads), 0);
    uint64_t wide = 0;
    len = sizeof(wide);
    CHECK_EQ(byName("stats.mutexes.ctl.max_num_thds", &wide, &len), EINVAL);
    CHECK_EQ(len, sizeof(uint32_t));
    CHECK_EQ(readValue("stats.mutexes.prof.num_wait", num_ops), 0);
    CHECK_EQ(num_ops, uint64_t(0));

    size_t value = 1;
    CHECK_EQ(readValue("stats.allocated", value), 0);
    CHECK_EQ(readValue("stats.background_thread.num_threads", value), 0);

    /// HPA and SEC statistics are zero.
    value = 1;
    CHECK_EQ(readValue("stats.arenas.0.hpa_shard.npageslabs", value), 0);
    CHECK_EQ(value, size_t(0));
    value = 1;
    CHECK_EQ(readValue("stats.arenas.4096.hpa_shard.nonfull_slabs.63.ndirty_huge", value), 0);
    CHECK_EQ(value, size_t(0));
    uint64_t counter = 1;
    CHECK_EQ(readValue("stats.arenas.0.hpa_shard.nhugifies", counter), 0);
    CHECK_EQ(counter, uint64_t(0));
    value = 1;
    CHECK_EQ(readValue("stats.arenas.1.hpa_sec_overfills", value), 0);
    CHECK_EQ(value, size_t(0));
    CHECK_EQ(byName("stats.arenas.0.hpa_shard.npageslabs", nullptr, nullptr, nullptr, 1), EPERM);
}

TEST(Mallctl, CreateDestroy)
{
    /// Arenas 2 and 3 do not exist, so `arenas.create` appends index 4 (the ctl's count).
    unsigned num_arenas = 0;
    CHECK_EQ(readValue("arenas.narenas", num_arenas), 0);
    unsigned idx = 0;
    size_t len = sizeof(idx);
    uint64_t wrong = 0;
    size_t wrong_len = sizeof(wrong);
    CHECK_EQ(byName("arenas.create", &wrong, &wrong_len), EINVAL);
    CHECK_EQ(wrong_len, size_t(0));
    CHECK_EQ(byName("arenas.create", &idx, &len), 0);
    CHECK_EQ(idx, num_arenas);
    unsigned num_arenas_after = 0;
    CHECK_EQ(readValue("arenas.narenas", num_arenas_after), 0);
    CHECK_EQ(num_arenas_after, num_arenas + 1);

    char name[64];
    /// Auto arenas cannot be reset or destroyed.
    CHECK_EQ(byName("arena.0.reset", nullptr, nullptr), EFAULT);
    CHECK_EQ(byName("arena.0.destroy", nullptr, nullptr), EFAULT);
    CHECK_EQ(byName("arena.0.reset", &idx, &len), EPERM);

    uint64_t epoch = 1;
    len = sizeof(epoch);
    CHECK_EQ(byName("epoch", nullptr, nullptr, &epoch, len), 0);
    bool initialized = false;
    std::snprintf(name, sizeof(name), "arena.%u.initialized", idx);
    CHECK_EQ(readValue(name, initialized), 0);
    CHECK_EQ(initialized, true);
    const char * sbrk = nullptr;
    std::snprintf(name, sizeof(name), "stats.arenas.%u.dss", idx);
    CHECK_EQ(readValue(name, sbrk), 0);
    CHECK_STREQ(sbrk, "secondary");
    size_t base = 0;
    std::snprintf(name, sizeof(name), "stats.arenas.%u.base", idx);
    CHECK_EQ(readValue(name, base), 0);
    CHECK_GT(base, size_t(0));

    ssize_t decay = 0;
    std::snprintf(name, sizeof(name), "arena.%u.dirty_decay_ms", idx);
    CHECK_EQ(readValue(name, decay), 0);
    CHECK_EQ(decay, options.dirty_decay_ms);
    ssize_t new_decay = 0;
    CHECK_EQ(byName(name, nullptr, nullptr, &new_decay, sizeof(new_decay)), 0);
    CHECK_EQ(readValue(name, decay), 0);
    CHECK_EQ(decay, ssize_t(0));
    new_decay = -2;
    CHECK_EQ(byName(name, nullptr, nullptr, &new_decay, sizeof(new_decay)), EFAULT);

    std::snprintf(name, sizeof(name), "arena.%u.purge", idx);
    CHECK_EQ(byName(name, nullptr, nullptr), 0);
    CHECK_EQ(byName("arena.4096.purge", nullptr, nullptr), 0);
    CHECK_EQ(byName("arena.4097.purge", nullptr, nullptr), 0);
    std::snprintf(name, sizeof(name), "arena.%u.reset", idx);
    CHECK_EQ(byName(name, nullptr, nullptr), 0);

    char arena_name[ARENA_NAME_LEN] = {};
    char * arena_name_ptr = arena_name;
    len = sizeof(arena_name_ptr);
    std::snprintf(name, sizeof(name), "arena.%u.name", idx);
    CHECK_EQ(byName(name, &arena_name_ptr, &len), 0);
    char expected_name[ARENA_NAME_LEN];
    std::snprintf(expected_name, sizeof(expected_name), "manual_%u", idx);
    CHECK_STREQ(arena_name, expected_name);
    CHECK_EQ(byName("arena.4096.name", &arena_name_ptr, &len), EINVAL);

    std::snprintf(name, sizeof(name), "arena.%u.destroy", idx);
    CHECK_EQ(byName(name, nullptr, nullptr), 0);
    std::snprintf(name, sizeof(name), "arena.%u.initialized", idx);
    CHECK_EQ(readValue(name, initialized), 0);
    CHECK_EQ(initialized, false);
    CHECK_EQ(readValue("arena.4097.initialized", initialized), 0);
    CHECK_EQ(initialized, true);
    /// The destroyed arena does not exist in `stats.arenas` any more; its stats are in the destroyed slot.
    std::snprintf(name, sizeof(name), "stats.arenas.%u.base", idx);
    CHECK_EQ(readValue(name, base), ENOENT);
    CHECK_EQ(readValue("stats.arenas.4097.base", base), 0);
    CHECK_EQ(base, size_t(0));

    /// The index is recycled.
    unsigned idx2 = 0;
    len = sizeof(idx2);
    CHECK_EQ(byName("arenas.create", &idx2, &len), 0);
    CHECK_EQ(idx2, idx);
    CHECK_EQ(readValue("arenas.narenas", num_arenas_after), 0);
    CHECK_EQ(num_arenas_after, num_arenas + 1);

    unsigned num_thread_cache_bins = 0;
    CHECK_EQ(readValue("arenas.nhbins", num_thread_cache_bins), 0);
    CHECK_EQ(num_thread_cache_bins, global_do_not_change_thread_cache_num_bins);
    size_t thread_cache_max = 0;
    CHECK_EQ(readValue("arenas.tcache_max", thread_cache_max), 0);
    CHECK_EQ(thread_cache_max, global_do_not_change_thread_cache_max_class);
    CHECK_EQ(readValue("arenas.dirty_decay_ms", decay), 0);
    CHECK_EQ(decay, options.dirty_decay_ms);
    size_t active = 1;
    CHECK_EQ(readValue("approximate_stats.active", active), 0);
    CHECK_EQ(readValue("stats.zero_reallocs", active), 0);
    CHECK_EQ(active, size_t(0));
}

TEST(Mallctl, NotImplementedLeaves)
{
    /// Leaves of later modules return ENOENT until they are implemented; dropped features always do.
    const char * name = nullptr;
    CHECK_EQ(readValue("thread.prof.name", name), ENOENT);
    CHECK_EQ(byName("prof.log_stop", nullptr, nullptr), ENOENT);
    CHECK_EQ(byName("experimental.hooks.install", nullptr, nullptr), ENOENT);
}

TEST(Mallctl, TreeShape)
{
    /// The number of children of every named level of jemalloc's tree.
    const MallctlNode & root = mallctl_super_root_node[0];
    CHECK_EQ(root.num_children, size_t(14));
    struct Expected
    {
        const char * name;
        size_t num_children;
    };
    for (Expected e :
         {Expected{"thread", 9},
          Expected{"thread.tcache", 4},
          Expected{"config", 13},
          Expected{"opt", 79},
          Expected{"opt.malloc_conf", 4},
          Expected{"tcache", 3},
          Expected{"arenas", 14},
          Expected{"prof", 11},
          Expected{"stats", 13},
          Expected{"stats.mutexes", 10},
          Expected{"stats.mutexes.ctl", 7},
          Expected{"experimental", 7},
          Expected{"experimental.hooks", 8}})
    {
        size_t numeric_path[MALLCTL_MAX_DEPTH];
        size_t len;
        REQUIRE(lookup(e.name, numeric_path, len) == 0);
        const MallctlNode * node = &root;
        for (size_t i = 0; i < len; ++i)
            node = &node->children[numeric_path[i]];
        CHECK_EQ(node->num_children, e.num_children);
    }
}
