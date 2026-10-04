/// Unit tests of the `mallctl` machinery (`Ctl.h`): name and MIB lookup, partial names and MIBs, error codes of the
/// access checks, the leaves that are implemented without other subsystems. The MIBs of all names are compared with
/// the reference jemalloc in ctl_names_oracle.cpp; the values pinned here are jemalloc's.

#include <allocator/Arenas.h>
#include <allocator/BackgroundThread.h>
#include <allocator/ExtentMap.h>
#include <allocator/BackgroundThread.h>
#include <allocator/Base.h>
#include <allocator/Ctl.h>
#include <allocator/CtlImpl.h>
#include <allocator/Options.h>
#include <allocator/Pages.h>
#include <allocator/SizeClasses.h>
#include <allocator/ExtentMap.h>
#include <allocator/ThreadCache.h>
#include <allocator/ThreadState.h>

#include "Test.h"

#include <atomic>

#include <cerrno>
#include <cstring>

using namespace jemalloc;

namespace
{

ThreadState test_tsd;

constexpr unsigned TEST_NARENAS = 4;

/// Boots the arena layer as the a0 initialization does (`arena_boot`, the global extent map, the arena table) and
/// creates real arenas (the ctl merges their stats on refresh).
void bootArenas()
{
    szBoot(default_sc_data, opt.cache_oblivious);
    REQUIRE(!arena_emap_global.init(b0get(), /* zeroed */ true));
    REQUIRE(!arenaBoot(&default_sc_data, b0get(), false));
    REQUIRE(!arenas_lock.init("arenas", MutexRank::ARENAS, MutexLockOrder::RankExclusive));
    narenas_auto = 1;
    manual_arena_base = 2;
    narenasTotalSet(1);
    REQUIRE(!backgroundThreadBoot0());
    REQUIRE(!backgroundThreadBoot1(nullptr, b0get()));
}

/// `arenaNew` enters and leaves reentrancy for `ind != 0`, which needs a tsd in a nominal state.
ThreadState & arenaTsd()
{
    static ThreadState tsd;
    tsd.state.store(tsd_state_nominal_slow, std::memory_order_relaxed);
    return tsd;
}

/// Arenas 0 and 1 exist (of `TEST_NARENAS`).
void setUpArenaTable()
{
    bootArenas();
    for (unsigned i = 0; i < 2; ++i)
        REQUIRE(arenaNew(&arenaTsd(), i, &arena_config_default) != nullptr);
    narenasTotalSet(TEST_NARENAS);
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
    REQUIRE(!ctlBoot());
    /// The mallctl tsd is nominal (slow): `arenas.create` creates arenas, which enters reentrancy.
    test_tsd.state.store(tsd_state_nominal_slow, std::memory_order_relaxed);
    test_tsd.rtree_ctx.init();
}

int nameToMib(const char * name, size_t * mib, size_t * miblen)
{
    setUp();
    return ctlNameToMib(test_tsd, name, mib, miblen);
}

/// Returns the error code; `*mib`/`*len` are filled on success.
int lookup(const char * name, size_t (&mib)[CTL_MAX_DEPTH], size_t & len)
{
    len = CTL_MAX_DEPTH;
    std::memset(mib, 0xff, sizeof(mib));
    return nameToMib(name, mib, &len);
}

int byName(const char * name, void * oldp, size_t * oldlenp, void * newp = nullptr, size_t newlen = 0)
{
    setUp();
    return ctlByName(test_tsd, name, oldp, oldlenp, newp, newlen);
}

template <typename T>
int readValue(const char * name, T & value)
{
    size_t len = sizeof(T);
    int ret = byName(name, &value, &len);
    if (ret == 0)
        CHECK_EQ(len, sizeof(T));
    return ret;
}

void checkMib(const char * name, std::initializer_list<size_t> expected)
{
    size_t mib[CTL_MAX_DEPTH];
    size_t len;
    int ret = lookup(name, mib, len);
    CHECK_EQ(ret, 0);
    CHECK_EQ(len, expected.size());
    size_t i = 0;
    for (size_t e : expected)
    {
        if (i < len && mib[i] != e)
        {
            std::fprintf(stderr, "%s: mib[%zu] = %zu, expected %zu\n", name, i, mib[i], e);
            CHECK(false);
        }
        ++i;
    }
}

void checkNoEntry(const char * name)
{
    size_t mib[CTL_MAX_DEPTH];
    size_t len;
    int ret = lookup(name, mib, len);
    if (ret != ENOENT)
    {
        std::fprintf(stderr, "\"%s\": expected ENOENT, got %d\n", name, ret);
        CHECK(false);
    }
}

}

TEST(Ctl, NameToMib)
{
    checkMib("version", {0});
    checkMib("epoch", {1});
    checkMib("thread.tcache.ncached_max.write", {4, 5, 3, 1});
    checkMib("config.xmalloc", {5, 12});
    checkMib("opt.abort", {6, 0});
    checkMib("opt.lg_extent_max_active_fit", {6, 55});
    checkMib("opt.malloc_conf.global_var_2_conf_harder", {6, 78, 3});
    checkMib("arena.0.decay", {8, 0, 1});
    checkMib("arena.4096.name", {8, 4096, 11});
    checkMib("arenas.bin.3.nshards", {9, 9, 3, 3});
    checkMib("arenas.lookup", {9, 13});
    checkMib("prof.stats", {10, 10});
    checkMib("stats.arenas.0.bins.5.mutex.max_num_thds", {11, 11, 0, 34, 5, 10, 6});
    checkMib("stats.arenas.4096.hpa_shard.nonfull_slabs.63.ndirty_huge", {11, 11, 4096, 38, 11, 63, 5});
    checkMib("stats.arenas.1.hpa_sec_dalloc_noflush", {11, 11, 1, 29});
    checkMib("stats.mutexes.reset", {11, 10, 9});
    checkMib("stats.zero_reallocs", {11, 12});
    checkMib("approximate_stats.active", {12, 0});
    checkMib("experimental.thread.activity_callback", {13, 6, 0});
}

TEST(Ctl, PartialNames)
{
    /// Partial names succeed in `mallctlnametomib`.
    checkMib("stats", {11});
    checkMib("stats.arenas", {11, 11});
    checkMib("stats.arenas.0", {11, 11, 0});
    checkMib("arena.4097", {8, 4097});
    checkMib("opt.malloc_conf", {6, 78});

    /// A too-small MIB buffer returns a truncated prefix successfully.
    size_t mib[CTL_MAX_DEPTH] = {};
    size_t len = 2;
    CHECK_EQ(nameToMib("stats.arenas.0.pactive", mib, &len), 0);
    CHECK_EQ(len, size_t(2));
    CHECK_EQ(mib[0], size_t(11));
    CHECK_EQ(mib[1], size_t(11));
    CHECK_EQ(mib[2], size_t(0));

    /// ... but they are not leaves.
    size_t value_len = sizeof(size_t);
    size_t value = 0;
    CHECK_EQ(byName("stats", &value, &value_len), ENOENT);
    CHECK_EQ(byName("stats.arenas.0", &value, &value_len), ENOENT);
}

TEST(Ctl, InvalidNames)
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
    /// Arena indices: `i <= narenas` (`narenas` is the alias of MALLCTL_ARENAS_ALL), 4096, 4097.
    checkMib("arena.4", {8, 4});
    checkNoEntry("arena.5");
    checkNoEntry("arena.4095");
    checkNoEntry("arena.4098");
    /// `stats.arenas.<i>`: only initialized arenas (at the last refresh), the alias, and 4096 (4097 only after a
    /// destroy).
    checkMib("stats.arenas.1", {11, 11, 1});
    checkNoEntry("stats.arenas.2");
    checkMib("stats.arenas.4", {11, 11, 4});
    checkMib("stats.arenas.4096", {11, 11, 4096});
    checkNoEntry("stats.arenas.4097");
    checkNoEntry("experimental.arenas.4097");
    checkMib("experimental.arenas.0.pactivep", {13, 2, 0, 0});
    /// `prof.stats.*` only exist with `prof` and `prof_stats`.
    checkNoEntry("prof.stats.bins.0");
    checkMib("prof.stats.bins", {10, 10, 0});
}

TEST(Ctl, LenientIndices)
{
    /// `malloc_strtoumax` skips whitespace, accepts a sign, and stops at the first non-digit (the element is still
    /// delimited by the dot).
    checkMib("stats.arenas.0x.pactive", {11, 11, 0, 5});
    checkMib("stats.arenas. 1.pactive", {11, 11, 1, 5});
    checkMib("stats.arenas.+1.pactive", {11, 11, 1, 5});
    checkMib("arenas.bin.007.size", {9, 9, 7, 0});
    checkNoEntry("stats.arenas..pactive");
}

TEST(Ctl, OffByOneIndices)
{
    /// jemalloc accepts `SC_NBINS` and `SC_NSIZES - SC_NBINS` (off by one) and rejects the next one.
    char name[64];
    std::snprintf(name, sizeof(name), "arenas.bin.%u.size", SC_NBINS);
    checkMib(name, {9, 9, SC_NBINS, 0});
    std::snprintf(name, sizeof(name), "arenas.bin.%u.size", SC_NBINS + 1);
    checkNoEntry(name);
    std::snprintf(name, sizeof(name), "arenas.lextent.%u.size", SC_NSIZES - SC_NBINS);
    checkMib(name, {9, 11, SC_NSIZES - SC_NBINS, 0});
    std::snprintf(name, sizeof(name), "arenas.lextent.%u.size", SC_NSIZES - SC_NBINS + 1);
    checkNoEntry(name);
    std::snprintf(name, sizeof(name), "stats.arenas.0.bins.%u.nmalloc", SC_NBINS);
    checkMib(name, {11, 11, 0, 34, SC_NBINS, 0});
    std::snprintf(name, sizeof(name), "stats.arenas.0.extents.%u.ndirty", SC_NPSIZES - 1);
    checkMib(name, {11, 11, 0, 36, SC_NPSIZES - 1, 0});
    std::snprintf(name, sizeof(name), "stats.arenas.0.extents.%u.ndirty", SC_NPSIZES);
    checkNoEntry(name);
    checkNoEntry("stats.arenas.0.hpa_shard.nonfull_slabs.64");

    /// The past-the-end entries read as zeros.
    size_t size = 1;
    std::snprintf(name, sizeof(name), "arenas.bin.%u.size", SC_NBINS);
    CHECK_EQ(readValue(name, size), 0);
    CHECK_EQ(size, size_t(0));
    size = 1;
    std::snprintf(name, sizeof(name), "arenas.lextent.%u.size", SC_NSIZES - SC_NBINS);
    CHECK_EQ(readValue(name, size), 0);
    CHECK_EQ(size, size_t(0));
}

TEST(Ctl, ByMib)
{
    setUp();
    size_t mib[CTL_MAX_DEPTH];
    size_t len;
    REQUIRE(lookup("arenas.page", mib, len) == 0);
    size_t page = 0;
    size_t page_len = sizeof(page);
    CHECK_EQ(ctlByMib(test_tsd, mib, len, &page, &page_len, nullptr, 0), 0);
    CHECK_EQ(page, PAGE);

    /// Partial MIB.
    CHECK_EQ(ctlByMib(test_tsd, mib, 1, &page, &page_len, nullptr, 0), ENOENT);
    /// Out of range.
    size_t bad[] = {9, 14};
    CHECK_EQ(ctlByMib(test_tsd, bad, 2, &page, &page_len, nullptr, 0), ENOENT);
    size_t bad_root[] = {14};
    CHECK_EQ(ctlByMib(test_tsd, bad_root, 1, &page, &page_len, nullptr, 0), ENOENT);
    /// Longer than the path.
    size_t longer[] = {9, 4, 0};
    CHECK_EQ(ctlByMib(test_tsd, longer, 3, &page, &page_len, nullptr, 0), ENOENT);
    /// Invalid index.
    size_t bad_index[] = {8, 4095, 0};
    CHECK_EQ(ctlByMib(test_tsd, bad_index, 3, &page, &page_len, nullptr, 0), ENOENT);
    /// An empty MIB is the root.
    CHECK_EQ(ctlByMib(test_tsd, mib, 0, &page, &page_len, nullptr, 0), ENOENT);

    /// The MIB of `arenas.bin.<i>.size` with every bin.
    REQUIRE(lookup("arenas.bin.0.size", mib, len) == 0);
    for (unsigned i = 0; i < SC_NBINS; ++i)
    {
        mib[2] = i;
        size_t size = 0;
        size_t size_len = sizeof(size);
        CHECK_EQ(ctlByMib(test_tsd, mib, len, &size, &size_len, nullptr, 0), 0);
        CHECK_EQ(size, sz::indexToSize(i));
    }
}

TEST(Ctl, MibNameToMib)
{
    setUp();
    size_t mib[CTL_MAX_DEPTH];
    size_t len;
    REQUIRE(lookup("stats.arenas", mib, len) == 0);
    REQUIRE(len == 2);

    size_t total = CTL_MAX_DEPTH;
    CHECK_EQ(ctlMibNameToMib(test_tsd, mib, 2, "0.bins.3.curregs", &total), 0);
    CHECK_EQ(total, size_t(6));
    CHECK_EQ(mib[2], size_t(0));
    CHECK_EQ(mib[3], size_t(34));
    CHECK_EQ(mib[4], size_t(3));
    CHECK_EQ(mib[5], size_t(3));

    /// The starting node must not be a leaf.
    size_t leaf_mib[CTL_MAX_DEPTH];
    REQUIRE(lookup("version", leaf_mib, len) == 0);
    total = CTL_MAX_DEPTH;
    CHECK_EQ(ctlMibNameToMib(test_tsd, leaf_mib, 1, "x", &total), ENOENT);
    /// Unknown name relative to the node.
    total = CTL_MAX_DEPTH;
    CHECK_EQ(ctlMibNameToMib(test_tsd, mib, 2, "0.no_such", &total), ENOENT);

    /// `ctlByMibName` reads the leaf (here relative to the root).
    total = CTL_MAX_DEPTH;
    const char * version = nullptr;
    size_t version_len = sizeof(version);
    CHECK_EQ(ctlByMibName(test_tsd, mib, 0, "version", &total, &version, &version_len, nullptr, 0), 0);
    CHECK_EQ(total, size_t(1));
    CHECK_STREQ(version, "5.3-RC");
    /// A partial name relative to a node is not a leaf.
    REQUIRE(lookup("stats.arenas", mib, len) == 0);
    total = CTL_MAX_DEPTH;
    CHECK_EQ(ctlByMibName(test_tsd, mib, 2, "0", &total, &version, &version_len, nullptr, 0), ENOENT);
}

TEST(Ctl, ReadWriteChecks)
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

    /// No read if `oldp` or `oldlenp` is null.
    CHECK_EQ(byName("epoch", nullptr, &len), 0);
    CHECK_EQ(byName("epoch", &epoch, nullptr), 0);

    /// WRITE: a wrong size is EINVAL; a write refreshes and the read returns the new epoch.
    uint64_t any = 12345;
    CHECK_EQ(byName("epoch", nullptr, nullptr, &any, 4), EINVAL);
    len = sizeof(epoch);
    CHECK_EQ(byName("epoch", &epoch, &len, &any, sizeof(any)), 0);
    CHECK_EQ(epoch, uint64_t(2));
    /// `newp == nullptr` with `newlen != 0` passes WRITE (and does not refresh).
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

TEST(Ctl, AccessCheckHelpers)
{
    int x = 0;
    size_t len = 0;
    CHECK_EQ(ctl::readOnly(nullptr, 0), 0);
    CHECK_EQ(ctl::readOnly(&x, 0), EPERM);
    CHECK_EQ(ctl::readOnly(nullptr, 1), EPERM);
    CHECK_EQ(ctl::writeOnly(nullptr, nullptr), 0);
    CHECK_EQ(ctl::writeOnly(&x, nullptr), EPERM);
    CHECK_EQ(ctl::writeOnly(nullptr, &len), EPERM);
    CHECK_EQ(ctl::readXorWrite(&x, &len, nullptr, 0), 0);
    CHECK_EQ(ctl::readXorWrite(nullptr, nullptr, &x, 4), 0);
    CHECK_EQ(ctl::readXorWrite(&x, nullptr, &x, 4), 0);
    CHECK_EQ(ctl::readXorWrite(&x, &len, &x, 4), EPERM);
    CHECK_EQ(ctl::readXorWrite(&x, &len, nullptr, 4), EPERM);
    CHECK_EQ(ctl::neitherReadNorWrite(nullptr, nullptr, nullptr, 0), 0);
    CHECK_EQ(ctl::neitherReadNorWrite(nullptr, &len, nullptr, 0), EPERM);
    CHECK_EQ(ctl::neitherReadNorWrite(nullptr, nullptr, nullptr, 1), EPERM);

    /// VERIFY_READ zeroes `*oldlenp` on failure.
    len = 3;
    CHECK_EQ(ctl::verifyRead<unsigned>(&x, &len), EINVAL);
    CHECK_EQ(len, size_t(0));
    len = sizeof(unsigned);
    CHECK_EQ(ctl::verifyRead<unsigned>(&x, &len), 0);
    CHECK_EQ(ctl::verifyRead<unsigned>(nullptr, &len), EINVAL);
    CHECK_EQ(len, size_t(0));
    CHECK_EQ(ctl::verifyRead<unsigned>(&x, nullptr), EINVAL);

    /// ASSURED_WRITE.
    int v = 0;
    int src = 7;
    CHECK_EQ(ctl::assuredWrite(nullptr, sizeof(int), v), EINVAL);
    CHECK_EQ(ctl::assuredWrite(&src, 3, v), EINVAL);
    CHECK_EQ(ctl::assuredWrite(&src, sizeof(int), v), 0);
    CHECK_EQ(v, 7);

    /// MIB_UNSIGNED.
    size_t mib[] = {0, size_t(UINT_MAX) + 1, UINT_MAX};
    unsigned u = 0;
    CHECK_EQ(ctl::mibUnsigned(mib, 1, u), EFAULT);
    CHECK_EQ(ctl::mibUnsigned(mib, 2, u), 0);
    CHECK_EQ(u, UINT_MAX);
}

TEST(Ctl, ConstantLeaves)
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
    CHECK_EQ(z, HUGEPAGE);
    CHECK_EQ(readValue("arenas.nbins", u), 0);
    CHECK_EQ(u, SC_NBINS);
    CHECK_EQ(readValue("arenas.nlextents", u), 0);
    CHECK_EQ(u, SC_NSIZES - SC_NBINS);
    CHECK_EQ(readValue("arenas.bin.0.size", z), 0);
    CHECK_EQ(z, size_t(8));
    CHECK_EQ(readValue("arenas.bin.0.nregs", u32), 0);
    CHECK_EQ(u32, bin_infos[0].nregs);
    CHECK_EQ(readValue("arenas.bin.0.slab_size", z), 0);
    CHECK_EQ(z, bin_infos[0].slab_size);
    CHECK_EQ(readValue("arenas.bin.0.nshards", u32), 0);
    CHECK_EQ(u32, 1u);
    CHECK_EQ(readValue("arenas.lextent.0.size", z), 0);
    CHECK_EQ(z, SC_LARGE_MINCLASS);
    /// `nregs` is a `uint32_t`: an 8-byte buffer gets 4 bytes and EINVAL.
    size_t wide = 0;
    size_t len = sizeof(wide);
    CHECK_EQ(byName("arenas.bin.0.nregs", &wide, &len), EINVAL);
    CHECK_EQ(len, sizeof(uint32_t));
}

TEST(Ctl, OptionLeaves)
{
    bool b = true;
    const char * str = nullptr;
    size_t z = 0;
    int64_t i64 = 0;
    ssize_t ss = 0;

    CHECK_EQ(readValue("opt.abort", b), 0);
    CHECK_EQ(b, opt.abort);
    CHECK_EQ(readValue("opt.junk", str), 0);
    CHECK_STREQ(str, opt.junk);
    CHECK_EQ(readValue("opt.dss", str), 0);
    CHECK_STREQ(str, "secondary");
    CHECK_EQ(readValue("opt.metadata_thp", str), 0);
    CHECK_STREQ(str, "disabled");
    CHECK_EQ(readValue("opt.thp", str), 0);
    CHECK_STREQ(str, "default");
    CHECK_EQ(readValue("opt.percpu_arena", str), 0);
    CHECK_STREQ(str, percpu_arena_mode_names[unsigned(opt.percpu_arena)]);
    CHECK_EQ(readValue("opt.zero_realloc", str), 0);
    CHECK_STREQ(str, zero_realloc_mode_names[unsigned(opt.zero_realloc_action)]);
    CHECK_EQ(readValue("opt.prof_time_resolution", str), 0);
    CHECK_STREQ(str, "default");
    CHECK_EQ(readValue("opt.hpa_hugify_style", str), 0);
    CHECK_STREQ(str, "lazy");
    CHECK_EQ(readValue("opt.prof_prefix", str), 0);
    CHECK_STREQ(str, "jeprof");
    CHECK_EQ(readValue("opt.oversize_threshold", z), 0);
    CHECK_EQ(z, opt.oversize_threshold);
    CHECK_EQ(readValue("opt.mutex_max_spin", i64), 0);
    CHECK_EQ(i64, int64_t(600));
    CHECK_EQ(readValue("opt.dirty_decay_ms", ss), 0);
    CHECK_EQ(ss, opt.dirty_decay_ms);
    CHECK_EQ(readValue("opt.stats_print_opts", str), 0);
    CHECK_STREQ(str, "");

    /// Options of disabled build features do not exist.
    CHECK_EQ(readValue("opt.utrace", b), ENOENT);
    CHECK_EQ(readValue("opt.xmalloc", b), ENOENT);
    CHECK_EQ(readValue("opt.lg_san_uaf_align", ss), config::uaf_detection ? 0 : ENOENT);
    /// `opt.malloc_conf.*` exist only when the corresponding source is set.
    CHECK_EQ(readValue("opt.malloc_conf.symlink", str), ENOENT);
    CHECK_EQ(readValue("opt.malloc_conf.env_var", str), opt.malloc_conf_env_var ? 0 : ENOENT);
    /// The condition is checked before READONLY.
    CHECK_EQ(byName("opt.utrace", nullptr, nullptr, nullptr, 1), ENOENT);
}

TEST(Ctl, ArenaSlots)
{
    unsigned narenas = 0;
    CHECK_EQ(readValue("arenas.narenas", narenas), 0);
    CHECK_EQ(narenas, TEST_NARENAS);

    bool initialized = false;
    CHECK_EQ(readValue("arena.0.initialized", initialized), 0);
    CHECK_EQ(initialized, true);
    CHECK_EQ(readValue("arena.1.initialized", initialized), 0);
    CHECK_EQ(initialized, true);
    CHECK_EQ(readValue("arena.2.initialized", initialized), 0);
    CHECK_EQ(initialized, false);
    /// The merged slot is always initialized; the destroyed one only after a destroy; `narenas` is the merged one.
    CHECK_EQ(readValue("arena.4096.initialized", initialized), 0);
    CHECK_EQ(initialized, true);
    CHECK_EQ(readValue("arena.4097.initialized", initialized), 0);
    CHECK_EQ(initialized, false);
    CHECK_EQ(readValue("arena.4.initialized", initialized), 0);
    CHECK_EQ(initialized, true);

    /// The cleared slot of the merged stats.
    const char * dss = nullptr;
    CHECK_EQ(readValue("stats.arenas.4096.dss", dss), 0);
    CHECK_STREQ(dss, "N/A");
    ssize_t decay = 0;
    CHECK_EQ(readValue("stats.arenas.4096.dirty_decay_ms", decay), 0);
    CHECK_EQ(decay, ssize_t(-1));
    unsigned nthreads = 1;
    CHECK_EQ(readValue("stats.arenas.0.nthreads", nthreads), 0);
    CHECK_EQ(nthreads, 0u);
    size_t allocated = 1;
    CHECK_EQ(readValue("stats.arenas.0.small.allocated", allocated), 0);
    CHECK_EQ(allocated, size_t(0));
}

TEST(Ctl, StatsLeaves)
{
    uint64_t epoch = 0;
    size_t len = sizeof(epoch);
    CHECK_EQ(byName("epoch", &epoch, &len, &epoch, sizeof(epoch)), 0);

    /// The profiling data of `ctl_mtx` is read during the refresh (while it is held, so it includes that lock).
    uint64_t num_ops = 0;
    CHECK_EQ(readValue("stats.mutexes.ctl.num_ops", num_ops), 0);
    CHECK_GT(num_ops, uint64_t(0));
    uint32_t max_num_thds = 1;
    CHECK_EQ(readValue("stats.mutexes.ctl.max_num_thds", max_num_thds), 0);
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

TEST(Ctl, CreateDestroy)
{
    /// Arenas 2 and 3 do not exist, so `arenas.create` appends index 4 (the ctl's count).
    unsigned narenas = 0;
    CHECK_EQ(readValue("arenas.narenas", narenas), 0);
    unsigned ind = 0;
    size_t len = sizeof(ind);
    uint64_t wrong = 0;
    size_t wrong_len = sizeof(wrong);
    CHECK_EQ(byName("arenas.create", &wrong, &wrong_len), EINVAL);
    CHECK_EQ(wrong_len, size_t(0));
    CHECK_EQ(byName("arenas.create", &ind, &len), 0);
    CHECK_EQ(ind, narenas);
    unsigned narenas_after = 0;
    CHECK_EQ(readValue("arenas.narenas", narenas_after), 0);
    CHECK_EQ(narenas_after, narenas + 1);

    char name[64];
    /// Auto arenas cannot be reset or destroyed.
    CHECK_EQ(byName("arena.0.reset", nullptr, nullptr), EFAULT);
    CHECK_EQ(byName("arena.0.destroy", nullptr, nullptr), EFAULT);
    CHECK_EQ(byName("arena.0.reset", &ind, &len), EPERM);

    uint64_t epoch = 1;
    len = sizeof(epoch);
    CHECK_EQ(byName("epoch", nullptr, nullptr, &epoch, len), 0);
    bool initialized = false;
    std::snprintf(name, sizeof(name), "arena.%u.initialized", ind);
    CHECK_EQ(readValue(name, initialized), 0);
    CHECK_EQ(initialized, true);
    const char * dss = nullptr;
    std::snprintf(name, sizeof(name), "stats.arenas.%u.dss", ind);
    CHECK_EQ(readValue(name, dss), 0);
    CHECK_STREQ(dss, "secondary");
    size_t base = 0;
    std::snprintf(name, sizeof(name), "stats.arenas.%u.base", ind);
    CHECK_EQ(readValue(name, base), 0);
    CHECK_GT(base, size_t(0));

    ssize_t decay = 0;
    std::snprintf(name, sizeof(name), "arena.%u.dirty_decay_ms", ind);
    CHECK_EQ(readValue(name, decay), 0);
    CHECK_EQ(decay, opt.dirty_decay_ms);
    ssize_t new_decay = 0;
    CHECK_EQ(byName(name, nullptr, nullptr, &new_decay, sizeof(new_decay)), 0);
    CHECK_EQ(readValue(name, decay), 0);
    CHECK_EQ(decay, ssize_t(0));
    new_decay = -2;
    CHECK_EQ(byName(name, nullptr, nullptr, &new_decay, sizeof(new_decay)), EFAULT);

    std::snprintf(name, sizeof(name), "arena.%u.purge", ind);
    CHECK_EQ(byName(name, nullptr, nullptr), 0);
    CHECK_EQ(byName("arena.4096.purge", nullptr, nullptr), 0);
    CHECK_EQ(byName("arena.4097.purge", nullptr, nullptr), 0);
    std::snprintf(name, sizeof(name), "arena.%u.reset", ind);
    CHECK_EQ(byName(name, nullptr, nullptr), 0);

    char arena_name[ARENA_NAME_LEN] = {};
    char * arena_name_ptr = arena_name;
    len = sizeof(arena_name_ptr);
    std::snprintf(name, sizeof(name), "arena.%u.name", ind);
    CHECK_EQ(byName(name, &arena_name_ptr, &len), 0);
    char expected_name[ARENA_NAME_LEN];
    std::snprintf(expected_name, sizeof(expected_name), "manual_%u", ind);
    CHECK_STREQ(arena_name, expected_name);
    CHECK_EQ(byName("arena.4096.name", &arena_name_ptr, &len), EINVAL);

    std::snprintf(name, sizeof(name), "arena.%u.destroy", ind);
    CHECK_EQ(byName(name, nullptr, nullptr), 0);
    std::snprintf(name, sizeof(name), "arena.%u.initialized", ind);
    CHECK_EQ(readValue(name, initialized), 0);
    CHECK_EQ(initialized, false);
    CHECK_EQ(readValue("arena.4097.initialized", initialized), 0);
    CHECK_EQ(initialized, true);
    /// The destroyed arena does not exist in `stats.arenas` any more; its stats are in the destroyed slot.
    std::snprintf(name, sizeof(name), "stats.arenas.%u.base", ind);
    CHECK_EQ(readValue(name, base), ENOENT);
    CHECK_EQ(readValue("stats.arenas.4097.base", base), 0);
    CHECK_EQ(base, size_t(0));

    /// The index is recycled.
    unsigned ind2 = 0;
    len = sizeof(ind2);
    CHECK_EQ(byName("arenas.create", &ind2, &len), 0);
    CHECK_EQ(ind2, ind);
    CHECK_EQ(readValue("arenas.narenas", narenas_after), 0);
    CHECK_EQ(narenas_after, narenas + 1);

    unsigned nhbins = 0;
    CHECK_EQ(readValue("arenas.nhbins", nhbins), 0);
    CHECK_EQ(nhbins, global_do_not_change_tcache_nbins);
    size_t tcache_max = 0;
    CHECK_EQ(readValue("arenas.tcache_max", tcache_max), 0);
    CHECK_EQ(tcache_max, global_do_not_change_tcache_maxclass);
    CHECK_EQ(readValue("arenas.dirty_decay_ms", decay), 0);
    CHECK_EQ(decay, opt.dirty_decay_ms);
    size_t active = 1;
    CHECK_EQ(readValue("approximate_stats.active", active), 0);
    CHECK_EQ(readValue("stats.zero_reallocs", active), 0);
    CHECK_EQ(active, size_t(0));
}

TEST(Ctl, NotImplementedLeaves)
{
    /// Leaves of later modules return ENOENT until they are implemented; dropped features always do.
    const char * name = nullptr;
    CHECK_EQ(readValue("thread.prof.name", name), ENOENT);
    CHECK_EQ(byName("prof.log_stop", nullptr, nullptr), ENOENT);
    CHECK_EQ(byName("experimental.hooks.install", nullptr, nullptr), ENOENT);
}

TEST(Ctl, TreeShape)
{
    /// The number of children of every named level of jemalloc's tree.
    const CtlNode & root = ctl_super_root_node[0];
    CHECK_EQ(root.nchildren, size_t(14));
    struct Expected
    {
        const char * name;
        size_t nchildren;
    };
    for (Expected e : {Expected{"thread", 9}, Expected{"thread.tcache", 4}, Expected{"config", 13}, Expected{"opt", 79},
                       Expected{"opt.malloc_conf", 4}, Expected{"tcache", 3}, Expected{"arenas", 14}, Expected{"prof", 11},
                       Expected{"stats", 13}, Expected{"stats.mutexes", 10}, Expected{"stats.mutexes.ctl", 7},
                       Expected{"experimental", 7}, Expected{"experimental.hooks", 8}})
    {
        size_t mib[CTL_MAX_DEPTH];
        size_t len;
        REQUIRE(lookup(e.name, mib, len) == 0);
        const CtlNode * node = &root;
        for (size_t i = 0; i < len; ++i)
            node = &node->children[mib[i]];
        CHECK_EQ(node->nchildren, e.nchildren);
    }
}
