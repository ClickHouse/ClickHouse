#include <allocator/SizeClasses.h>

namespace jemalloc
{

constinit size_t sz_large_pad = config::cache_oblivious ? PAGE : 0;

constinit std::array<BinInfo, SC_NBINS> bin_infos = default_bin_infos;

/// --- sc.c ----------------------------------------------------------------------------------------------------

namespace
{

/// jemalloc: sc_data_update_sc_slab_size
void scDataUpdateScSlabSize(SizeClass & sc, size_t reg_size, size_t pgs_guess)
{
    size_t min_pgs = reg_size / PAGE;
    if (reg_size % PAGE != 0)
        ++min_pgs;
    /// BITMAP_MAXBITS is actually determined by putting the smallest possible size-class on one page, so this can
    /// never be 0.
    size_t max_pgs = BITMAP_MAXBITS * reg_size / PAGE;

    JE_ASSERT(min_pgs <= max_pgs);
    JE_ASSERT(min_pgs > 0);
    JE_ASSERT(max_pgs >= 1);
    if (pgs_guess < min_pgs)
        sc.pgs = int(min_pgs);
    else if (pgs_guess > max_pgs)
        sc.pgs = int(max_pgs);
    else
        sc.pgs = int(pgs_guess);
}

}

/// jemalloc: sc_data_update_slab_size
void scDataUpdateSlabSize(SizeClassData & data, size_t begin, size_t end, int pgs)
{
    JE_ASSERT(data.initialized);
    for (int i = 0; i < data.nsizes; ++i)
    {
        SizeClass & sc = data.sc[i];
        if (!sc.bin)
            break;
        size_t reg_size = regSizeCompute(sc.lg_base, sc.lg_delta, sc.ndelta);
        if (begin <= reg_size && reg_size <= end)
            scDataUpdateScSlabSize(sc, reg_size, size_t(pgs)); /// A negative `pgs` becomes huge, as in jemalloc.
    }
}

/// jemalloc: sc_boot
void scBoot(SizeClassData & data)
{
    scDataInit(data);
}

/// --- sz.c ----------------------------------------------------------------------------------------------------

/// jemalloc: sz_boot
void szBoot(const SizeClassData & sc_data, bool cache_oblivious)
{
    sz_large_pad = cache_oblivious ? PAGE : 0;

    /// The tables are compile-time constants; check that they are what `sz_boot` would compute from `sc_data`.
    if constexpr (config::debug)
    {
        unsigned pind = 0;
        for (unsigned i = 0; i < SC_NSIZES; ++i)
        {
            const SizeClass & sc = sc_data.sc[i];
            size_t size = (size_t(1) << sc.lg_base) + (size_t(sc.ndelta) << sc.lg_delta);
            JE_ASSERT(sz_index2size_tab[i] == size);
            if (sc.psz)
            {
                JE_ASSERT(sz_pind2sz_tab[pind] == size);
                ++pind;
            }
        }
        JE_ASSERT(pind == SC_NPSIZES);
        JE_ASSERT(sz_pind2sz_tab[SC_NPSIZES] == sc_data.large_maxclass + PAGE);
    }
}

namespace sz
{

/// jemalloc: sz_psz_quantize_floor
size_t pszQuantizeFloor(size_t size)
{
    JE_ASSERT(size > 0);
    JE_ASSERT((size & PAGE_MASK) == 0);

    pszind_t pind = psz2ind(size - sz_large_pad + 1);
    if (pind == 0)
    {
        /// Avoid underflow. This short-circuit would also do the right thing for all sizes in the range for which
        /// there are PAGE-spaced size classes, but it's simplest to just handle the one case that would cause
        /// erroneous results.
        return size;
    }
    size_t ret = pind2sz(pind - 1) + sz_large_pad;
    JE_ASSERT(ret <= size);
    return ret;
}

/// jemalloc: sz_psz_quantize_ceil
size_t pszQuantizeCeil(size_t size)
{
    JE_ASSERT(size > 0);
    JE_ASSERT(size - sz_large_pad <= SC_LARGE_MAXCLASS);
    JE_ASSERT((size & PAGE_MASK) == 0);

    size_t ret = pszQuantizeFloor(size);
    if (ret < size)
    {
        /// Skip a quantization that may have an adequately large extent, because under-sized extents may be mixed
        /// in. This only happens when an unusual size is requested, i.e. for aligned allocation.
        ret = pind2sz(psz2ind(ret - sz_large_pad + 1)) + sz_large_pad;
    }
    return ret;
}

}

/// --- bin_info.c, bin.c ---------------------------------------------------------------------------------------

/// jemalloc: bin_info_boot
void binInfoBoot(const SizeClassData & sc_data, const unsigned * bin_shard_sizes)
{
    JE_ASSERT(sc_data.initialized);
    detail::binInfosInit(sc_data, bin_shard_sizes, bin_infos.data());
}

/// jemalloc: bin_update_shard_size
bool binUpdateShardSize(unsigned * bin_shard_sizes, size_t start_size, size_t end_size, size_t nshards)
{
    if (nshards > BIN_SHARDS_MAX || nshards == 0)
        return true;

    if (start_size > SC_SMALL_MAXCLASS)
        return false;
    if (end_size > SC_SMALL_MAXCLASS)
        end_size = SC_SMALL_MAXCLASS;

    /// Compute the index since this may happen before sz init.
    szind_t ind1 = sz::sizeToIndexCompute(start_size);
    szind_t ind2 = sz::sizeToIndexCompute(end_size);
    for (unsigned i = ind1; i <= ind2; ++i)
        bin_shard_sizes[i] = unsigned(nshards);

    return false;
}

/// jemalloc: bin_shard_sizes_boot
void binShardSizesBoot(unsigned * bin_shard_sizes)
{
    /// Load the default number of shards.
    for (unsigned i = 0; i < SC_NBINS; ++i)
        bin_shard_sizes[i] = N_BIN_SHARDS_DEFAULT;
}

}
