/// Compares the tcache settings with the reference jemalloc (linked as `lib_jemalloc.a`, same page size): the boot
/// globals (`arenas.tcache_max`, `arenas.nhbins`), the default `ncached_max` of every bin
/// (`thread.tcache.ncached_max.read_sizeclass`), and the effect of `thread.tcache.ncached_max.write` and
/// `thread.tcache.max` on them, against `tcacheBoot`, `tcacheGetDefaultNcachedMax`, `tcacheBinInfoSettingsParse` and
/// the read rule of `tcacheBinNcachedMaxRead`.

#include <allocator/Conf.h>
#include <allocator/Options.h>
#include <allocator/SizeClasses.h>
#include <allocator/ThreadCache.h>

#include "Test.h"

#include <cerrno>
#include <cstring>

extern "C"
{
int je_mallctl(const char * name, void * oldp, size_t * oldlenp, void * newp, size_t newlen);

/// The reference library is built with libunwind (prof backtraces are never taken here).
int unw_backtrace(void **, int)
{
    return 0;
}
}

using namespace jemalloc;

namespace
{

size_t refRead(size_t bin_size)
{
    size_t ncached_max = 12345;
    size_t len = sizeof(ncached_max);
    REQUIRE(je_mallctl("thread.tcache.ncached_max.read_sizeclass", &ncached_max, &len, &bin_size, sizeof(bin_size)) == 0);
    return ncached_max;
}

/// The value `thread.tcache.ncached_max.read_sizeclass` reports for a bin of a tcache with these settings.
unsigned modelRead(const CacheBinInfo * infos, unsigned nbins, szind_t i)
{
    return (i < nbins && infos[i].ncached_max > 0) ? infos[i].ncached_max : 0;
}

void checkAll(const CacheBinInfo * infos, unsigned nbins)
{
    for (szind_t i = 0; i < TCACHE_NBINS_MAX; ++i)
    {
        size_t size = sz::indexToSize(i);
        CHECK_EQ(refRead(size), size_t(modelRead(infos, nbins, i)));
        /// A non-class size reads the bin it rounds up to.
        CHECK_EQ(refRead(size - 1), size_t(modelRead(infos, nbins, i)));
    }
    size_t too_big = TCACHE_MAXCLASS_LIMIT + 1;
    size_t ncached_max = 0;
    size_t len = sizeof(ncached_max);
    CHECK_EQ(je_mallctl("thread.tcache.ncached_max.read_sizeclass", &ncached_max, &len, &too_big, sizeof(too_big)), EINVAL);
}

}

TEST(ThreadCacheOracle, Boot)
{
    REQUIRE(!tcacheBoot(nullptr, nullptr));

    size_t ref_tcache_max = 0;
    size_t len = sizeof(ref_tcache_max);
    REQUIRE(je_mallctl("arenas.tcache_max", &ref_tcache_max, &len, nullptr, 0) == 0);
    CHECK_EQ(global_do_not_change_tcache_maxclass, ref_tcache_max);

    unsigned ref_nhbins = 0;
    len = sizeof(ref_nhbins);
    REQUIRE(je_mallctl("arenas.nhbins", &ref_nhbins, &len, nullptr, 0) == 0);
    CHECK_EQ(global_do_not_change_tcache_nbins, ref_nhbins);

    /// The slab region counts that the defaults derive from.
    for (unsigned i = 0; i < SC_NBINS; ++i)
    {
        char name[64];
        snprintf(name, sizeof(name), "arenas.bin.%u.nregs", i);
        uint32_t nregs = 0;
        len = sizeof(nregs);
        REQUIRE(je_mallctl(name, &nregs, &len, nullptr, 0) == 0);
        CHECK_EQ(bin_infos[i].nregs, nregs);
    }

    checkAll(tcacheGetDefaultNcachedMax(), global_do_not_change_tcache_nbins);
}

TEST(ThreadCacheOracle, NcachedMaxWrite)
{
    REQUIRE(!tcacheBoot(nullptr, nullptr));
    CacheBinInfo infos[TCACHE_NBINS_MAX];
    for (szind_t i = 0; i < TCACHE_NBINS_MAX; ++i)
        infos[i] = tcacheGetDefaultNcachedMax()[i];
    unsigned nbins = global_do_not_change_tcache_nbins;

    /// The inputs of `test/unit/ncached_max.c` and a few more.
    const char * inputs[] = {
        "8-128:1|160-160:11|170-320:22|224-8388609:0",
        "0-112:8",
        "1-1:9000",
        "4096-4096:3|3000-2000:7",
        "16384-65536:5",
    };
    for (const char * input : inputs)
    {
        const char * p = input;
        REQUIRE(je_mallctl("thread.tcache.ncached_max.write", nullptr, nullptr, &p, sizeof(p)) == 0);
        REQUIRE(!tcacheBinInfoSettingsParse(input, strlen(input), [&](szind_t i, uint16_t n) { infos[i].init(n); }));
        checkAll(infos, nbins);
    }

    /// `thread.tcache.max` keeps the per-bin settings (also of the bins beyond the old limit).
    const size_t maxes[] = {1024, 100, TCACHE_MAXCLASS_LIMIT * 2, 32768, 8};
    for (size_t new_max : maxes)
    {
        REQUIRE(je_mallctl("thread.tcache.max", nullptr, nullptr, &new_max, sizeof(new_max)) == 0);
        size_t clipped = sz::s2u(new_max > TCACHE_MAXCLASS_LIMIT ? TCACHE_MAXCLASS_LIMIT : new_max);
        nbins = sz::sizeToIndex(clipped) + 1;
        size_t ref_max = 0;
        size_t len = sizeof(ref_max);
        REQUIRE(je_mallctl("thread.tcache.max", &ref_max, &len, nullptr, 0) == 0);
        CHECK_EQ(ref_max, sz::indexToSize(nbins - 1));
        checkAll(infos, nbins);
    }

    /// A malformed input is rejected and changes nothing.
    const char * bad = "8-16";
    CHECK_EQ(je_mallctl("thread.tcache.ncached_max.write", nullptr, nullptr, &bad, sizeof(bad)), EINVAL);
    CHECK(tcacheBinInfoSettingsParse(bad, strlen(bad), [](szind_t, uint16_t) {}));
    checkAll(infos, nbins);
}
