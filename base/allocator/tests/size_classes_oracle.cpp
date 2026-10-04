/// Compares the size class tables and every size computation with jemalloc's C implementation
/// (`size_classes_oracle_ref.c`, linked with the reference `lib_jemalloc.a`), for a dense set of sizes, all
/// alignments, every combination of `disable_large_size_classes` and `cache_oblivious`, and randomized
/// `slab_sizes` / `bin_shards` updates.

#include <allocator/SizeClasses.h>

#include "Test.h"

#include <vector>

using namespace jemalloc;

extern "C"
{
size_t ref_constant(int which);
void ref_boot(bool cache_oblivious);
void ref_sz_boot(bool cache_oblivious);
void ref_sc_data_init();
void ref_sc_update_slab_size(size_t begin, size_t end, int pgs);
void ref_bin_shard_sizes_boot();
bool ref_bin_update_shard_size(size_t start, size_t end, size_t nshards);
unsigned ref_bin_shard(unsigned i);
void ref_bin_info_boot();
void ref_sc_summary(size_t * out);
void ref_sc(unsigned i, int * out);
void ref_bin_info(unsigned i, size_t * out);
void ref_set_disable_large_size_classes(bool value);
size_t ref_large_pad();
size_t ref_index2size_tab(unsigned i);
unsigned ref_size2index_tab(unsigned i);
size_t ref_pind2sz_tab(unsigned i);
unsigned ref_size2index(size_t size);
unsigned ref_size2index_compute(size_t size);
unsigned ref_size2index_lookup(size_t size);
void ref_size2index_usize_fastpath(size_t size, unsigned * ind, size_t * usize);
size_t ref_index2size_compute(unsigned index);
size_t ref_index2size_unsafe(unsigned index);
size_t ref_index2size(unsigned index);
size_t ref_s2u(size_t size);
size_t ref_s2u_compute(size_t size);
size_t ref_sa2u(size_t size, size_t alignment);
unsigned ref_psz2ind(size_t psz);
size_t ref_pind2sz(unsigned pind);
size_t ref_pind2sz_compute(unsigned pind);
size_t ref_psz2u(size_t psz);
size_t ref_psz_quantize_floor(size_t size);
size_t ref_psz_quantize_ceil(size_t size);
bool ref_can_use_slab(size_t size);
bool ref_large_size_classes_disabled();
uint32_t ref_div_magic(size_t d);
size_t ref_div_compute(size_t d, size_t n);
}

namespace
{

/// The state of the C++ side that mirrors the reference's static `sc_data` and shard sizes.
SizeClassData sc_data;
unsigned shards[SC_NBINS];

void bootBoth(bool cache_oblivious)
{
    ref_boot(cache_oblivious);
    scBoot(sc_data);
    binShardSizesBoot(shards);
    szBoot(sc_data, cache_oblivious);
    binInfoBoot(sc_data, shards);
}

void setDisableLargeSizeClasses(bool value)
{
    ref_set_disable_large_size_classes(value);
    opt.disable_large_size_classes = value;
}

template <bool UseTree>
void bitmapInfoFields(const BitmapInfoImpl<UseTree> & info, size_t * out)
{
    out[0] = info.nbits;
    if constexpr (UseTree)
    {
        out[1] = info.nlevels;
        for (unsigned l = 0; l <= BITMAP_MAX_LEVELS; ++l)
            out[2 + l] = info.levels[l].group_offset;
    }
    else
        out[1] = info.ngroups;
}

void compareScData()
{
    size_t ref_summary[13];
    ref_sc_summary(ref_summary);
    CHECK_EQ(ref_summary[0], size_t(sc_data.ntiny));
    CHECK_EQ(ref_summary[1], size_t(sc_data.nlbins));
    CHECK_EQ(ref_summary[2], size_t(sc_data.nbins));
    CHECK_EQ(ref_summary[3], size_t(sc_data.nsizes));
    CHECK_EQ(ref_summary[4], size_t(sc_data.lg_ceil_nsizes));
    CHECK_EQ(ref_summary[5], size_t(sc_data.npsizes));
    CHECK_EQ(ref_summary[6], size_t(sc_data.lg_tiny_maxclass));
    CHECK_EQ(ref_summary[7], sc_data.lookup_maxclass);
    CHECK_EQ(ref_summary[8], sc_data.small_maxclass);
    CHECK_EQ(ref_summary[9], size_t(sc_data.lg_large_minclass));
    CHECK_EQ(ref_summary[10], sc_data.large_minclass);
    CHECK_EQ(ref_summary[11], sc_data.large_maxclass);
    CHECK_EQ(ref_summary[12], size_t(sc_data.initialized));

    for (unsigned i = 0; i < SC_NSIZES; ++i)
    {
        int ref[8];
        ref_sc(i, ref);
        const SizeClass & sc = sc_data.sc[i];
        CHECK_EQ(ref[0], sc.index);
        CHECK_EQ(ref[1], sc.lg_base);
        CHECK_EQ(ref[2], sc.lg_delta);
        CHECK_EQ(ref[3], sc.ndelta);
        CHECK_EQ(ref[4], int(sc.psz));
        CHECK_EQ(ref[5], int(sc.bin));
        CHECK_EQ(ref[6], sc.pgs);
        CHECK_EQ(ref[7], sc.lg_delta_lookup);
    }
}

void compareBinInfos()
{
    for (unsigned i = 0; i < SC_NBINS; ++i)
    {
        size_t ref[12] = {};
        size_t own[12] = {};
        ref_bin_info(i, ref);
        const BinInfo & info = bin_infos[i];
        own[0] = info.reg_size;
        own[1] = info.slab_size;
        own[2] = info.nregs;
        own[3] = info.n_shards;
        bitmapInfoFields(info.bitmap_info, own + 4);
        for (unsigned k = 0; k < 12; ++k)
            CHECK_EQ(own[k], ref[k]);
        CHECK_EQ(shards[i], ref_bin_shard(i));
    }
}

void compareTables()
{
    for (unsigned i = 0; i < SC_NSIZES; ++i)
        CHECK_EQ(sz_index2size_tab[i], ref_index2size_tab(i));
    for (unsigned i = 0; i < sz_size2index_tab.size(); ++i)
        CHECK_EQ(unsigned(sz_size2index_tab[i]), ref_size2index_tab(i));
    for (unsigned i = 0; i <= SC_NPSIZES; ++i)
        CHECK_EQ(sz_pind2sz_tab[i], ref_pind2sz_tab(i));
}

std::vector<size_t> makeSizes()
{
    std::vector<size_t> sizes;
    for (size_t size = 0; size <= 70000; ++size)
        sizes.push_back(size);
    for (unsigned lg = 0; lg < 64; ++lg)
    {
        size_t base = size_t(1) << lg;
        /// The power of two and the other class boundaries of its group, with small deltas.
        for (size_t quarter = 0; quarter < 4; ++quarter)
        {
            size_t point = base + quarter * (base >> 2);
            for (int delta = -20; delta <= 20; ++delta)
                sizes.push_back(point + size_t(ptrdiff_t(delta)));
            for (int pages = -3; pages <= 3; ++pages)
                sizes.push_back(point + size_t(ptrdiff_t(pages)) * PAGE);
        }
    }
    for (int delta = -100; delta <= 100; ++delta)
    {
        sizes.push_back(SC_LARGE_MAXCLASS + size_t(ptrdiff_t(delta)));
        sizes.push_back(SC_LARGE_MAXCLASS + PAGE + size_t(ptrdiff_t(delta)));
        sizes.push_back(size_t(ptrdiff_t(delta)));
    }
    for (int pages = -10; pages <= 10; ++pages)
        sizes.push_back(SC_LARGE_MAXCLASS + size_t(ptrdiff_t(pages)) * PAGE);
    /// A pseudo-random sample of all magnitudes.
    uint64_t x = 0x9E3779B97F4A7C15ULL;
    for (int i = 0; i < 200000; ++i)
    {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        sizes.push_back(size_t(x >> (x % 64)));
    }
    return sizes;
}

void compareLookups(const std::vector<size_t> & sizes, bool with_alignments)
{
    CHECK_EQ(sz_large_pad, ref_large_pad());
    CHECK_EQ(sz::largeSizeClassesDisabled(), ref_large_size_classes_disabled());

    for (unsigned i = 0; i < SC_NSIZES; ++i)
    {
        CHECK_EQ(sz::indexToSizeCompute(i), ref_index2size_compute(i));
        CHECK_EQ(sz::indexToSizeUnsafe(i), ref_index2size_unsafe(i));
        if (!sz::largeSizeClassesDisabled() || i <= sz::sizeToIndex(USIZE_GROW_SLOW_THRESHOLD))
            CHECK_EQ(sz::indexToSize(i), ref_index2size(i));
    }
    for (unsigned p = 0; p <= SC_NPSIZES; ++p)
    {
        CHECK_EQ(sz::pind2szCompute(p), ref_pind2sz_compute(p));
        CHECK_EQ(sz::pind2sz(p), ref_pind2sz(p));
    }

    size_t failures_before = size_t(allocator_test::failureCount());
    for (size_t size : sizes)
    {
        CHECK_EQ(sz::sizeToIndex(size), ref_size2index(size));
        CHECK_EQ(sz::sizeToIndexCompute(size), ref_size2index_compute(size));
        CHECK_EQ(sz::s2u(size), ref_s2u(size));
        CHECK_EQ(sz::s2uCompute(size), ref_s2u_compute(size));
        CHECK_EQ(sz::canUseSlab(size), ref_can_use_slab(size));
        if (size <= SC_LOOKUP_MAXCLASS)
        {
            CHECK_EQ(sz::sizeToIndexLookup(size), ref_size2index_lookup(size));
            szind_t ind;
            size_t usize;
            unsigned ref_ind;
            size_t ref_usize;
            sz::sizeToIndexUsizeFastpath(size, &ind, &usize);
            ref_size2index_usize_fastpath(size, &ref_ind, &ref_usize);
            CHECK_EQ(ind, ref_ind);
            CHECK_EQ(usize, ref_usize);
        }
        if (size > 0)
        {
            CHECK_EQ(sz::psz2ind(size), ref_psz2ind(size));
            CHECK_EQ(sz::psz2u(size), ref_psz2u(size));
        }
        if (size > 0 && (size & PAGE_MASK) == 0 && size >= sz_large_pad && size - sz_large_pad <= SC_LARGE_MAXCLASS)
        {
            CHECK_EQ(sz::pszQuantizeFloor(size), ref_psz_quantize_floor(size));
            CHECK_EQ(sz::pszQuantizeCeil(size), ref_psz_quantize_ceil(size));
        }
        if (with_alignments)
        {
            for (unsigned lg_align = 0; lg_align < 64; ++lg_align)
                CHECK_EQ(sz::sa2u(size, size_t(1) << lg_align), ref_sa2u(size, size_t(1) << lg_align));
        }
        /// Do not flood the output.
        REQUIRE(size_t(allocator_test::failureCount()) < failures_before + 100);
    }
}

}

TEST(SizeClassesOracle, Constants)
{
    REQUIRE(ref_constant(3) == PAGE);
    CHECK_EQ(ref_constant(0), size_t(SC_NSIZES));
    CHECK_EQ(ref_constant(1), size_t(SC_NBINS));
    CHECK_EQ(ref_constant(2), size_t(SC_NPSIZES));
    CHECK_EQ(ref_constant(4), SC_SMALL_MAXCLASS);
    CHECK_EQ(ref_constant(5), SC_LARGE_MINCLASS);
    CHECK_EQ(ref_constant(6), SC_LARGE_MAXCLASS);
    CHECK_EQ(ref_constant(7), SC_LOOKUP_MAXCLASS);
    CHECK_EQ(ref_constant(8), size_t(SC_LG_SLAB_MAXREGS));
    CHECK_EQ(ref_constant(9), size_t(LG_BITMAP_MAXBITS));
    CHECK_EQ(ref_constant(10), BITMAP_MAXBITS);
    CHECK_EQ(ref_constant(11), BITMAP_GROUPS_MAX);
    CHECK_EQ(ref_constant(12), sizeof(BitmapInfo));
    CHECK_EQ(ref_constant(13), sizeof(BinInfo));
    CHECK_EQ(ref_constant(14), size_t(USIZE_GROW_SLOW_THRESHOLD));
    CHECK_EQ(ref_constant(15), size_t(SC_NTINY));
    CHECK_EQ(ref_constant(16), sz_size2index_tab.size());
    CHECK_EQ(ref_constant(17), size_t(BIN_SHARDS_MAX));
}

TEST(SizeClassesOracle, Tables)
{
    bootBoth(true);
    compareScData();
    compareBinInfos();
    compareTables();

    /// The compile-time defaults are the same as the boot result.
    sc_data = default_sc_data;
    compareScData();
    for (unsigned i = 0; i < SC_NBINS; ++i)
    {
        size_t ref[12] = {};
        size_t own[12] = {};
        ref_bin_info(i, ref);
        const BinInfo & info = default_bin_infos[i];
        own[0] = info.reg_size;
        own[1] = info.slab_size;
        own[2] = info.nregs;
        own[3] = info.n_shards;
        bitmapInfoFields(info.bitmap_info, own + 4);
        for (unsigned k = 0; k < 12; ++k)
            CHECK_EQ(own[k], ref[k]);
    }
}

TEST(SizeClassesOracle, Lookups)
{
    std::vector<size_t> sizes = makeSizes();
    for (bool cache_oblivious : {true, false})
    {
        for (bool disable_large : {true, false})
        {
            bootBoth(cache_oblivious);
            setDisableLargeSizeClasses(disable_large);
            compareLookups(sizes, /* with_alignments */ false);
        }
    }
    setDisableLargeSizeClasses(true);
    bootBoth(true);
}

TEST(SizeClassesOracle, AlignedLookups)
{
    /// sa2u with every power of two alignment, on a smaller set of sizes.
    std::vector<size_t> all = makeSizes();
    std::vector<size_t> sizes;
    for (size_t i = 0; i < all.size(); ++i)
        if (all[i] > 70000 || all[i] % 7 == 0 || all[i] < 300)
            sizes.push_back(all[i]);
    for (bool cache_oblivious : {true, false})
    {
        for (bool disable_large : {true, false})
        {
            bootBoth(cache_oblivious);
            setDisableLargeSizeClasses(disable_large);
            compareLookups(sizes, /* with_alignments */ true);
        }
    }
    setDisableLargeSizeClasses(true);
    bootBoth(true);
}

TEST(SizeClassesOracle, Div)
{
    for (size_t d = 2; d <= 600000; ++d)
    {
        DivInfo div;
        div.init(d);
        CHECK_EQ(div.magic, ref_div_magic(d));
        if (d % 997 == 0)
        {
            for (size_t k = 0; k * d < (size_t(1) << 32) && k < 100000; k += 1 + k / 8)
                CHECK_EQ(div.compute(k * d), ref_div_compute(d, k * d));
        }
    }
    for (unsigned i = 0; i < SC_NBINS; ++i)
    {
        DivInfo div;
        div.init(bin_infos[i].reg_size);
        for (size_t k = 0; k < bin_infos[i].nregs; ++k)
            CHECK_EQ(div.compute(k * bin_infos[i].reg_size), ref_div_compute(bin_infos[i].reg_size, k * bin_infos[i].reg_size));
    }
}

TEST(SizeClassesOracle, SlabSizesAndShards)
{
    uint64_t x = 0x2545F4914F6CDD1DULL;
    auto next = [&x]
    {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        return x;
    };
    auto randomSize = [&]
    {
        uint64_t r = next();
        switch (r % 4)
        {
            case 0: return size_t(r >> 40) % (SC_SMALL_MAXCLASS + 100);
            case 1: return bin_infos[(r >> 8) % SC_NBINS].reg_size + size_t((r >> 16) % 3) - 1;
            case 2: return size_t(r >> (r % 64));
            default: return size_t(0);
        }
    };

    for (int round = 0; round < 300; ++round)
    {
        bootBoth(true);
        int nupdates = int(next() % 6);
        for (int u = 0; u < nupdates; ++u)
        {
            size_t begin = randomSize();
            size_t end = (next() % 4 == 0) ? ~size_t(0) : randomSize();
            uint64_t r = next();
            int pgs = (r % 5 == 0) ? int(int64_t(r >> 32)) : int((r >> 8) % 300) - 2;
            if (next() % 10 == 0)
            {
                /// `slab_sizes:default`.
                ref_sc_data_init();
                scDataInit(sc_data);
            }
            ref_sc_update_slab_size(begin, end, pgs);
            scDataUpdateSlabSize(sc_data, begin, end, pgs);

            size_t start = randomSize();
            size_t stop = randomSize();
            size_t nshards = size_t(next() % 70);
            bool ref_error = ref_bin_update_shard_size(start, stop, nshards);
            bool own_error = binUpdateShardSize(shards, start, stop, nshards);
            CHECK_EQ(own_error, ref_error);
        }
        ref_bin_info_boot();
        binInfoBoot(sc_data, shards);
        compareScData();
        compareBinInfos();
        REQUIRE(allocator_test::failureCount() < 100);
    }
    bootBoth(true);
}
