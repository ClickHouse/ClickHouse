/// Compares the tcache settings with the reference jemalloc (linked as `lib_jemalloc.a`, same page size): the boot
/// globals (`arenas.tcache_max`, `arenas.nhbins`), the default `num_cached_max` of every bin
/// (`thread.tcache.ncached_max.read_sizeclass`), and the effect of `thread.tcache.ncached_max.write` and
/// `thread.tcache.max` on them, against `threadCacheBoot`, `threadCacheGetDefaultNumCachedMax`, `threadCacheBinInfoSettingsParse` and
/// the read rule of `threadCacheBinNumCachedMaxRead`.

#include <allocator/MallocConf.h>
#include <allocator/Options.h>
#include <allocator/SizeClasses.h>
#include <allocator/ThreadCache.h>

#include "Test.h"

#include <cerrno>
#include <cstring>

extern "C" {
int je_mallctl(const char * name, void * old_value, size_t * old_length_ptr, void * new_value, size_t new_length);

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
    size_t num_cached_max = 12345;
    size_t len = sizeof(num_cached_max);
    REQUIRE(je_mallctl("thread.tcache.ncached_max.read_sizeclass", &num_cached_max, &len, &bin_size, sizeof(bin_size)) == 0);
    return num_cached_max;
}

/// The value `thread.tcache.ncached_max.read_sizeclass` reports for a bin of a tcache with these settings.
unsigned modelRead(const CacheBinInfo * infos, unsigned num_bins, SizeClassIdx i)
{
    return (i < num_bins && infos[i].num_cached_max > 0) ? infos[i].num_cached_max : 0;
}

void checkAll(const CacheBinInfo * infos, unsigned num_bins)
{
    for (SizeClassIdx i = 0; i < THREAD_CACHE_NUM_BINS_MAX; ++i)
    {
        size_t size = size_classes::indexToSize(i);
        CHECK_EQ(refRead(size), size_t(modelRead(infos, num_bins, i)));
        /// A non-class size reads the bin it rounds up to.
        CHECK_EQ(refRead(size - 1), size_t(modelRead(infos, num_bins, i)));
    }
    size_t too_big = THREAD_CACHE_MAX_CLASS_LIMIT + 1;
    size_t num_cached_max = 0;
    size_t len = sizeof(num_cached_max);
    CHECK_EQ(je_mallctl("thread.tcache.ncached_max.read_sizeclass", &num_cached_max, &len, &too_big, sizeof(too_big)), EINVAL);
}

}

TEST(ThreadCacheOracle, Boot)
{
    REQUIRE(!threadCacheBoot(nullptr, nullptr));

    size_t ref_thread_cache_max = 0;
    size_t len = sizeof(ref_thread_cache_max);
    REQUIRE(je_mallctl("arenas.tcache_max", &ref_thread_cache_max, &len, nullptr, 0) == 0);
    CHECK_EQ(global_do_not_change_thread_cache_max_class, ref_thread_cache_max);

    unsigned ref_num_thread_cache_bins = 0;
    len = sizeof(ref_num_thread_cache_bins);
    REQUIRE(je_mallctl("arenas.nhbins", &ref_num_thread_cache_bins, &len, nullptr, 0) == 0);
    CHECK_EQ(global_do_not_change_thread_cache_num_bins, ref_num_thread_cache_bins);

    /// The slab region counts that the defaults derive from.
    for (unsigned i = 0; i < SIZE_CLASS_NUM_BINS; ++i)
    {
        char name[64];
        snprintf(name, sizeof(name), "arenas.bin.%u.nregs", i);
        uint32_t num_regions = 0;
        len = sizeof(num_regions);
        REQUIRE(je_mallctl(name, &num_regions, &len, nullptr, 0) == 0);
        CHECK_EQ(bin_infos[i].num_regions, num_regions);
    }

    checkAll(threadCacheGetDefaultNumCachedMax(), global_do_not_change_thread_cache_num_bins);
}

TEST(ThreadCacheOracle, NumCachedMaxWrite)
{
    REQUIRE(!threadCacheBoot(nullptr, nullptr));
    CacheBinInfo infos[THREAD_CACHE_NUM_BINS_MAX];
    for (SizeClassIdx i = 0; i < THREAD_CACHE_NUM_BINS_MAX; ++i)
        infos[i] = threadCacheGetDefaultNumCachedMax()[i];
    unsigned num_bins = global_do_not_change_thread_cache_num_bins;

    /// The inputs of `test/unit/num_cached_max.c` and a few more.
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
        REQUIRE(!threadCacheBinInfoSettingsParse(input, strlen(input), [&](SizeClassIdx i, uint16_t n) { infos[i].init(n); }));
        checkAll(infos, num_bins);
    }

    /// `thread.tcache.max` keeps the per-bin settings (also of the bins beyond the old limit).
    const size_t maxes[] = {1024, 100, THREAD_CACHE_MAX_CLASS_LIMIT * 2, 32768, 8};
    for (size_t new_max : maxes)
    {
        REQUIRE(je_mallctl("thread.tcache.max", nullptr, nullptr, &new_max, sizeof(new_max)) == 0);
        size_t clipped = size_classes::sizeToUsableSize(new_max > THREAD_CACHE_MAX_CLASS_LIMIT ? THREAD_CACHE_MAX_CLASS_LIMIT : new_max);
        num_bins = size_classes::sizeToIndex(clipped) + 1;
        size_t ref_max = 0;
        size_t len = sizeof(ref_max);
        REQUIRE(je_mallctl("thread.tcache.max", &ref_max, &len, nullptr, 0) == 0);
        CHECK_EQ(ref_max, size_classes::indexToSize(num_bins - 1));
        checkAll(infos, num_bins);
    }

    /// A malformed input is rejected and changes nothing.
    const char * bad = "8-16";
    CHECK_EQ(je_mallctl("thread.tcache.ncached_max.write", nullptr, nullptr, &bad, sizeof(bad)), EINVAL);
    CHECK(threadCacheBinInfoSettingsParse(bad, strlen(bad), [](SizeClassIdx, uint16_t) { }));
    checkAll(infos, num_bins);
}
