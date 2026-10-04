#pragma once

/// Size classes, size computations, bin metadata and region index division.
/// jemalloc: `sc.h`/`sc.c`, `sz.h`/`sz.c`, `bin_info.h`/`bin_info.c`, `div.h`/`div.c`, `bin_update_shard_size` and
/// `bin_shard_sizes_boot` from `bin.c`.
///
/// The size class table (`sc_data_t`) depends only on compile-time constants, except the slab page counts which the
/// `slab_sizes` option can change at boot. Therefore:
/// - the default table `default_sc_data` and the lookup tables `sz_index2size_tab`, `sz_size2index_tab`,
///   `sz_pind2sz_tab` (which do not depend on slab sizes) are compile-time constants;
/// - `bin_infos` (slab sizes, shards) and `sz_large_pad` (`cache_oblivious`) are constant-initialized with the
///   defaults and recomputed by `binInfoBoot` / `szBoot` at boot, after the options are parsed.

#include <allocator/Bitmap.h>
#include <allocator/Common.h>
#include <allocator/Options.h>
#include <allocator/SizeClassConstants.h>

#include <array>

namespace jemalloc
{

/// --- Size class generation (sc.c) ------------------------------------------------------------------------------

/// jemalloc: sc_t
struct SizeClass
{
    /// Size class index, or -1 if not a valid size class.
    int index;
    /// Lg group base size (no deltas added).
    int lg_base;
    /// Lg delta to previous size class.
    int lg_delta;
    /// Delta multiplier. size == 1<<lg_base + ndelta<<lg_delta
    int ndelta;
    /// True if the size class is a multiple of the page size.
    bool psz;
    /// True if the size class is a small, bin, size class.
    bool bin;
    /// The slab page count if a small bin size class, 0 otherwise.
    int pgs;
    /// Same as lg_delta if a lookup table size class, 0 otherwise.
    int lg_delta_lookup;
};

/// jemalloc: sc_data_t
struct SizeClassData
{
    /// Number of tiny size classes.
    unsigned ntiny;
    /// Number of bins supported by the lookup table.
    int nlbins;
    /// Number of small size class bins.
    int nbins;
    /// Number of size classes.
    int nsizes;
    /// Number of bits required to store NSIZES.
    int lg_ceil_nsizes;
    /// Number of size classes that are a multiple of PAGE.
    unsigned npsizes;
    /// Lg of maximum tiny size class (or -1, if none).
    int lg_tiny_maxclass;
    /// Maximum size class included in lookup table.
    size_t lookup_maxclass;
    /// Maximum small size class.
    size_t small_maxclass;
    /// Lg of minimum large size class.
    int lg_large_minclass;
    /// The minimum large size class.
    size_t large_minclass;
    /// Maximum (large) size class.
    size_t large_maxclass;
    /// True if the data has been initialized (for debugging only).
    bool initialized;

    SizeClass sc[SC_NSIZES];
};

/// jemalloc: reg_size_compute
constexpr size_t regSizeCompute(int lg_base, int lg_delta, int ndelta)
{
    return (size_t(1) << lg_base) + (size_t(ndelta) << lg_delta);
}

namespace detail
{

/// jemalloc: slab_size. Returns the number of pages in the slab: the smallest page count whose size is an exact
/// multiple of the region size.
constexpr int slabSize(int lg_page, int lg_base, int lg_delta, int ndelta)
{
    size_t page = size_t(1) << lg_page;
    size_t reg_size = regSizeCompute(lg_base, lg_delta, ndelta);

    size_t try_slab_size = page;
    size_t try_nregs = try_slab_size / reg_size;
    size_t perfect_slab_size = 0;
    bool perfect = false;
    while (!perfect)
    {
        perfect_slab_size = try_slab_size;
        size_t perfect_nregs = try_nregs;
        try_slab_size += page;
        try_nregs = try_slab_size / reg_size;
        if (perfect_slab_size == perfect_nregs * reg_size)
            perfect = true;
    }
    return int(perfect_slab_size / page);
}

/// jemalloc: size_class
constexpr void sizeClass(
    SizeClass & sc, int lg_max_lookup, int lg_page, int lg_ngroup, int index, int lg_base, int lg_delta, int ndelta)
{
    sc.index = index;
    sc.lg_base = lg_base;
    sc.lg_delta = lg_delta;
    sc.ndelta = ndelta;
    size_t size = regSizeCompute(lg_base, lg_delta, ndelta);
    sc.psz = (size % (size_t(1) << lg_page) == 0);
    if (size < (size_t(1) << (lg_page + lg_ngroup)))
    {
        sc.bin = true;
        sc.pgs = slabSize(lg_page, lg_base, lg_delta, ndelta);
    }
    else
    {
        sc.bin = false;
        sc.pgs = 0;
    }
    if (size <= (size_t(1) << lg_max_lookup))
        sc.lg_delta_lookup = lg_delta;
    else
        sc.lg_delta_lookup = 0;
}

/// jemalloc: size_classes
constexpr void sizeClasses(
    SizeClassData & sc_data, size_t lg_ptr_size, int lg_quantum, int lg_tiny_min, int lg_max_lookup, int lg_page, int lg_ngroup)
{
    int ptr_bits = (1 << lg_ptr_size) * 8;
    int ngroup = (1 << lg_ngroup);
    int ntiny = 0;
    int nlbins = 0;
    int lg_tiny_maxclass = -1;
    int nbins = 0;
    int npsizes = 0;

    int index = 0;

    int ndelta = 0;
    int lg_base = lg_tiny_min;
    int lg_delta = lg_base;

    /// Outputs that we update as we go.
    size_t lookup_maxclass = 0;
    size_t small_maxclass = 0;
    int lg_large_minclass = 0;
    size_t large_maxclass = 0;

    /// Tiny size classes.
    while (lg_base < lg_quantum)
    {
        SizeClass & sc = sc_data.sc[index];
        sizeClass(sc, lg_max_lookup, lg_page, lg_ngroup, index, lg_base, lg_delta, ndelta);
        if (sc.lg_delta_lookup != 0)
            nlbins = index + 1;
        if (sc.psz)
            ++npsizes;
        if (sc.bin)
            ++nbins;
        ++ntiny;
        /// Final written value is correct.
        lg_tiny_maxclass = lg_base;
        ++index;
        lg_delta = lg_base;
        ++lg_base;
    }

    /// First non-tiny (pseudo) group.
    if (ntiny != 0)
    {
        SizeClass & sc = sc_data.sc[index];
        /// The first non-tiny size class has an unusual encoding.
        --lg_base;
        ndelta = 1;
        sizeClass(sc, lg_max_lookup, lg_page, lg_ngroup, index, lg_base, lg_delta, ndelta);
        ++index;
        ++lg_base;
        ++lg_delta;
        if (sc.psz)
            ++npsizes;
        if (sc.bin)
            ++nbins;
    }
    while (ndelta < ngroup)
    {
        SizeClass & sc = sc_data.sc[index];
        sizeClass(sc, lg_max_lookup, lg_page, lg_ngroup, index, lg_base, lg_delta, ndelta);
        ++index;
        ++ndelta;
        if (sc.psz)
            ++npsizes;
        if (sc.bin)
            ++nbins;
    }

    /// All remaining groups.
    lg_base = lg_base + lg_ngroup;
    while (lg_base < ptr_bits - 1)
    {
        ndelta = 1;
        int ndelta_limit;
        if (lg_base == ptr_bits - 2)
            ndelta_limit = ngroup - 1;
        else
            ndelta_limit = ngroup;
        while (ndelta <= ndelta_limit)
        {
            SizeClass & sc = sc_data.sc[index];
            sizeClass(sc, lg_max_lookup, lg_page, lg_ngroup, index, lg_base, lg_delta, ndelta);
            if (sc.lg_delta_lookup != 0)
            {
                nlbins = index + 1;
                /// Final written value is correct.
                lookup_maxclass = (size_t(1) << lg_base) + (size_t(ndelta) << lg_delta);
            }
            if (sc.psz)
                ++npsizes;
            if (sc.bin)
            {
                ++nbins;
                /// Final written value is correct.
                small_maxclass = (size_t(1) << lg_base) + (size_t(ndelta) << lg_delta);
                if (lg_ngroup > 0)
                    lg_large_minclass = lg_base + 1;
                else
                    lg_large_minclass = lg_base + 2;
            }
            large_maxclass = (size_t(1) << lg_base) + (size_t(ndelta) << lg_delta);
            ++index;
            ++ndelta;
        }
        ++lg_base;
        ++lg_delta;
    }
    /// Additional outputs.
    int nsizes = index;
    unsigned lg_ceil_nsizes = lgCeil(size_t(nsizes));

    /// Fill in the output data.
    sc_data.ntiny = unsigned(ntiny);
    sc_data.nlbins = nlbins;
    sc_data.nbins = nbins;
    sc_data.nsizes = nsizes;
    sc_data.lg_ceil_nsizes = int(lg_ceil_nsizes);
    sc_data.npsizes = unsigned(npsizes);
    sc_data.lg_tiny_maxclass = lg_tiny_maxclass;
    sc_data.lookup_maxclass = lookup_maxclass;
    sc_data.small_maxclass = small_maxclass;
    sc_data.lg_large_minclass = lg_large_minclass;
    sc_data.large_minclass = size_t(1) << lg_large_minclass;
    sc_data.large_maxclass = large_maxclass;
}

}

/// jemalloc: sc_data_init
constexpr void scDataInit(SizeClassData & sc_data)
{
    detail::sizeClasses(sc_data, LG_SIZEOF_PTR, LG_QUANTUM, SC_LG_TINY_MIN, SC_LG_MAX_LOOKUP, LG_PAGE, SC_LG_NGROUP);
    sc_data.initialized = true;
}

/// Updates slab sizes of the small classes with sizes in [begin, end] to be `pgs` pages in length, if possible.
/// Otherwise, does its best to accommodate the request (clamps to the valid range for each class).
/// jemalloc: sc_data_update_slab_size
void scDataUpdateSlabSize(SizeClassData & data, size_t begin, size_t end, int pgs);

/// jemalloc: sc_boot
void scBoot(SizeClassData & data);

namespace detail
{

consteval SizeClassData makeDefaultSizeClassData()
{
    SizeClassData data{};
    scDataInit(data);
    return data;
}

}

/// The default size class table (`sc_boot` result before any `slab_sizes` option is applied).
inline constexpr SizeClassData default_sc_data = detail::makeDefaultSizeClassData();

/// The two computations of the size class parameters (incremental and macros) must agree (`sc.c:238-252`).
static_assert(default_sc_data.nsizes == int(SC_NSIZES));
static_assert(default_sc_data.nbins == int(SC_NBINS));
static_assert(default_sc_data.ntiny == SC_NTINY);
static_assert(default_sc_data.npsizes == SC_NPSIZES);
static_assert(default_sc_data.lg_tiny_maxclass == SC_LG_TINY_MAXCLASS);
static_assert(default_sc_data.lookup_maxclass == SC_LOOKUP_MAXCLASS);
static_assert(default_sc_data.small_maxclass == SC_SMALL_MAXCLASS);
static_assert(default_sc_data.large_minclass == SC_LARGE_MINCLASS);
static_assert(default_sc_data.lg_large_minclass == int(SC_LG_LARGE_MINCLASS));
static_assert(default_sc_data.large_maxclass == SC_LARGE_MAXCLASS);
static_assert(default_sc_data.lg_ceil_nsizes == int(lgCeilConst(SC_NSIZES)));

/// --- Options that affect size computations -------------------------------------------------------------------

/// `opt.disable_large_size_classes` (default true, settable through the `disable_large_size_classes` conf key) is in
/// Options.h.

/// Padding for large allocations: PAGE when `opt.cache_oblivious` (to enable cache index randomization), 0 otherwise.
/// Set by `szBoot`; initialized for the default `cache_oblivious = true`.
/// jemalloc: sz_large_pad
extern constinit size_t sz_large_pad;

/// --- Lookup tables (sz.c) --------------------------------------------------------------------------------------

namespace detail
{

/// jemalloc: sz_boot_pind2sz_tab
consteval std::array<size_t, SC_NPSIZES + 1> makePind2SizeTab(const SizeClassData & sc_data)
{
    std::array<size_t, SC_NPSIZES + 1> tab{};
    unsigned pind = 0;
    for (unsigned i = 0; i < SC_NSIZES; ++i)
    {
        const SizeClass & sc = sc_data.sc[i];
        if (sc.psz)
        {
            tab[pind] = (size_t(1) << sc.lg_base) + (size_t(sc.ndelta) << sc.lg_delta);
            ++pind;
        }
    }
    /// jemalloc writes `tab[pind]` (not `tab[i]`) in this loop; it does not matter because pind == SC_NPSIZES here.
    for (unsigned i = pind; i <= SC_NPSIZES; ++i)
        tab[pind] = sc_data.large_maxclass + PAGE;
    return tab;
}

/// jemalloc: sz_boot_index2size_tab
consteval std::array<size_t, SC_NSIZES> makeIndex2SizeTab(const SizeClassData & sc_data)
{
    std::array<size_t, SC_NSIZES> tab{};
    for (unsigned i = 0; i < SC_NSIZES; ++i)
    {
        const SizeClass & sc = sc_data.sc[i];
        tab[i] = (size_t(1) << sc.lg_base) + (size_t(sc.ndelta) << sc.lg_delta);
    }
    return tab;
}

inline constexpr size_t SIZE2INDEX_TAB_SIZE = (SC_LOOKUP_MAXCLASS >> SC_LG_TINY_MIN) + 1;

/// jemalloc: sz_boot_size2index_tab. Entry k is the index of the smallest class >= 8k.
consteval std::array<uint8_t, SIZE2INDEX_TAB_SIZE> makeSize2IndexTab(const SizeClassData & sc_data)
{
    std::array<uint8_t, SIZE2INDEX_TAB_SIZE> tab{};
    size_t dst_max = SIZE2INDEX_TAB_SIZE;
    size_t dst_ind = 0;
    for (unsigned sc_ind = 0; sc_ind < SC_NSIZES && dst_ind < dst_max; ++sc_ind)
    {
        const SizeClass & sc = sc_data.sc[sc_ind];
        size_t sz = (size_t(1) << sc.lg_base) + (size_t(sc.ndelta) << sc.lg_delta);
        size_t max_ind = ((sz + (size_t(1) << SC_LG_TINY_MIN) - 1) >> SC_LG_TINY_MIN);
        for (; dst_ind <= max_ind && dst_ind < dst_max; ++dst_ind)
            tab[dst_ind] = uint8_t(sc_ind);
    }
    return tab;
}

}

/// These tables only depend on the class sizes, which are compile-time constants (the `slab_sizes` option changes
/// only slab page counts), so unlike jemalloc they are built at compile time; `szBoot` verifies them in debug builds.

/// jemalloc: sz_pind2sz_tab
alignas(CACHELINE) inline constexpr std::array<size_t, SC_NPSIZES + 1> sz_pind2sz_tab = detail::makePind2SizeTab(default_sc_data);
/// jemalloc: sz_index2size_tab
alignas(CACHELINE) inline constexpr std::array<size_t, SC_NSIZES> sz_index2size_tab = detail::makeIndex2SizeTab(default_sc_data);
/// jemalloc: sz_size2index_tab. Compressed by dividing sizes by the tiny min size.
alignas(CACHELINE) inline constexpr std::array<uint8_t, detail::SIZE2INDEX_TAB_SIZE> sz_size2index_tab
    = detail::makeSize2IndexTab(default_sc_data);

/// jemalloc: sz_boot. Sets `sz_large_pad`; the tables are constant (see above).
void szBoot(const SizeClassData & sc_data, bool cache_oblivious);

/// --- Size computations (sz.h) ----------------------------------------------------------------------------------

namespace sz
{

/// jemalloc: sz_large_size_classes_disabled
JE_ALWAYS_INLINE bool largeSizeClassesDisabled()
{
    return opt.disable_large_size_classes;
}

/// Page size to page size index. jemalloc: sz_psz2ind
JE_ALWAYS_INLINE pszind_t psz2ind(size_t psz)
{
    JE_ASSERT(psz > 0);
    if (JE_UNLIKELY(psz > SC_LARGE_MAXCLASS))
        return SC_NPSIZES;
    /// x is the lg of the first base >= psz.
    pszind_t x = lgCeil(psz);
    /// The offset from the first group whose classes are all multiples of PAGE (base == PAGE * SC_NGROUP);
    /// starts from 1 for (PAGE * SC_NGROUP, PAGE * SC_NGROUP * 2].
    pszind_t off_to_first_ps_rg = (x < SC_LG_NGROUP + LG_PAGE) ? 0 : x - (SC_LG_NGROUP + LG_PAGE);
    /// Delta for off_to_first_ps_rg == 1 is PAGE, and it doubles for every next group.
    pszind_t lg_delta = (off_to_first_ps_rg == 0) ? LG_PAGE : LG_PAGE + (off_to_first_ps_rg - 1);
    /// (psz - 1) handles the case psz % (1 << lg_delta) == 0.
    pszind_t rg_inner_off = pszind_t(((psz - 1)) >> lg_delta) & (SC_NGROUP - 1);
    pszind_t base_ind = off_to_first_ps_rg << SC_LG_NGROUP;
    pszind_t ind = base_ind + rg_inner_off;
    return ind;
}

/// jemalloc: sz_pind2sz_compute
constexpr size_t pind2szCompute(pszind_t pind)
{
    if (JE_UNLIKELY(pind == SC_NPSIZES))
        return SC_LARGE_MAXCLASS + PAGE;
    size_t grp = pind >> SC_LG_NGROUP;
    size_t mod = pind & ((size_t(1) << SC_LG_NGROUP) - 1);

    size_t grp_size_mask = ~((!!grp) - size_t(1));
    size_t grp_size = ((size_t(1) << (LG_PAGE + (SC_LG_NGROUP - 1))) << grp) & grp_size_mask;

    size_t shift = (grp == 0) ? 1 : grp;
    size_t lg_delta = shift + (LG_PAGE - 1);
    size_t mod_size = (mod + 1) << lg_delta;

    size_t sz = grp_size + mod_size;
    return sz;
}

/// jemalloc: sz_pind2sz_lookup
JE_ALWAYS_INLINE size_t pind2szLookup(pszind_t pind)
{
    size_t ret = sz_pind2sz_tab[pind];
    JE_ASSERT(ret == pind2szCompute(pind));
    return ret;
}

/// Page size index to page size. jemalloc: sz_pind2sz
JE_ALWAYS_INLINE size_t pind2sz(pszind_t pind)
{
    JE_ASSERT(pind < SC_NPSIZES + 1);
    return pind2szLookup(pind);
}

/// Page size to usable page size. jemalloc: sz_psz2u
JE_ALWAYS_INLINE size_t psz2u(size_t psz)
{
    if (JE_UNLIKELY(psz > SC_LARGE_MAXCLASS))
        return SC_LARGE_MAXCLASS + PAGE;
    size_t x = lgFloor((psz << 1) - 1);
    size_t lg_delta = (x < SC_LG_NGROUP + LG_PAGE + 1) ? LG_PAGE : x - SC_LG_NGROUP - 1;
    size_t delta = size_t(1) << lg_delta;
    size_t delta_mask = delta - 1;
    size_t usize = (psz + delta_mask) & ~delta_mask;
    return usize;
}

/// jemalloc: sz_size2index_compute_inline (and sz_size2index_compute, which is the same).
JE_ALWAYS_INLINE constexpr szind_t sizeToIndexCompute(size_t size)
{
    if (JE_UNLIKELY(size > SC_LARGE_MAXCLASS))
        return SC_NSIZES;

    if (size == 0)
        return 0;

    if constexpr (SC_NTINY != 0)
    {
        if (size <= (size_t(1) << SC_LG_TINY_MAXCLASS))
        {
            szind_t lg_tmin = SC_LG_TINY_MAXCLASS - SC_NTINY + 1;
            szind_t lg_ceil = lgFloor(pow2Ceil(size));
            return (lg_ceil < lg_tmin ? 0 : lg_ceil - lg_tmin);
        }
    }

    szind_t x = lgFloor((size << 1) - 1);
    szind_t shift = (x < SC_LG_NGROUP + LG_QUANTUM) ? 0 : x - (SC_LG_NGROUP + LG_QUANTUM);
    szind_t grp = shift << SC_LG_NGROUP;

    szind_t lg_delta = (x < SC_LG_NGROUP + LG_QUANTUM + 1) ? LG_QUANTUM : x - SC_LG_NGROUP - 1;

    size_t delta_inverse_mask = size_t(-1) << lg_delta;
    szind_t mod = szind_t((((size - 1) & delta_inverse_mask) >> lg_delta) & ((size_t(1) << SC_LG_NGROUP) - 1));

    szind_t index = SC_NTINY + grp + mod;
    return index;
}

/// jemalloc: sz_size2index_lookup_impl
JE_ALWAYS_INLINE szind_t sizeToIndexLookupImpl(size_t size)
{
    JE_ASSERT(size <= SC_LOOKUP_MAXCLASS);
    return sz_size2index_tab[(size + (size_t(1) << SC_LG_TINY_MIN) - 1) >> SC_LG_TINY_MIN];
}

/// jemalloc: sz_size2index_lookup
JE_ALWAYS_INLINE szind_t sizeToIndexLookup(size_t size)
{
    szind_t ret = sizeToIndexLookupImpl(size);
    JE_ASSERT(ret == sizeToIndexCompute(size));
    return ret;
}

/// Size to size class index; SC_NSIZES if the size is too large. jemalloc: sz_size2index
JE_ALWAYS_INLINE szind_t sizeToIndex(size_t size)
{
    if (JE_LIKELY(size <= SC_LOOKUP_MAXCLASS))
        return sizeToIndexLookup(size);
    return sizeToIndexCompute(size);
}

/// jemalloc: sz_index2size_compute_inline (and sz_index2size_compute, which is the same).
JE_ALWAYS_INLINE constexpr size_t indexToSizeCompute(szind_t index)
{
    if constexpr (SC_NTINY > 0)
    {
        if (index < SC_NTINY)
            return size_t(1) << (SC_LG_TINY_MAXCLASS - SC_NTINY + 1 + index);
    }

    size_t reduced_index = index - SC_NTINY;
    size_t grp = reduced_index >> SC_LG_NGROUP;
    size_t mod = reduced_index & ((size_t(1) << SC_LG_NGROUP) - 1);

    size_t grp_size_mask = ~((!!grp) - size_t(1));
    size_t grp_size = ((size_t(1) << (LG_QUANTUM + (SC_LG_NGROUP - 1))) << grp) & grp_size_mask;

    size_t shift = (grp == 0) ? 1 : grp;
    size_t lg_delta = shift + (LG_QUANTUM - 1);
    size_t mod_size = (mod + 1) << lg_delta;

    size_t usize = grp_size + mod_size;
    return usize;
}

/// jemalloc: sz_index2size_lookup_impl
JE_ALWAYS_INLINE size_t indexToSizeLookupImpl(szind_t index)
{
    return sz_index2size_tab[index];
}

/// jemalloc: sz_index2size_lookup
JE_ALWAYS_INLINE size_t indexToSizeLookup(szind_t index)
{
    size_t ret = indexToSizeLookupImpl(index);
    JE_ASSERT(ret == indexToSizeCompute(index));
    return ret;
}

/// Size class index to size, for any index (also large classes when they are disabled).
/// jemalloc: sz_index2size_unsafe
JE_ALWAYS_INLINE size_t indexToSizeUnsafe(szind_t index)
{
    JE_ASSERT(index < SC_NSIZES);
    return indexToSizeLookup(index);
}

/// Size class index to size. With large size classes disabled, only indices up to the class of
/// USIZE_GROW_SLOW_THRESHOLD are meaningful (asserted). jemalloc: sz_index2size
JE_ALWAYS_INLINE size_t indexToSize(szind_t index)
{
    JE_ASSERT(!largeSizeClassesDisabled() || index <= sizeToIndex(USIZE_GROW_SLOW_THRESHOLD));
    size_t size = indexToSizeUnsafe(index);
    /// With large size classes disabled, the usize above SC_LARGE_MINCLASS should grow by PAGE. However, for sizes
    /// in [SC_LARGE_MINCLASS, USIZE_GROW_SLOW_THRESHOLD] the class gap is just PAGE, and tcache caches up to
    /// USIZE_GROW_SLOW_THRESHOLD, hence this bound.
    JE_ASSERT(!largeSizeClassesDisabled() || size <= USIZE_GROW_SLOW_THRESHOLD);
    return size;
}

/// Index and usable size for `size <= SC_LOOKUP_MAXCLASS`. jemalloc: sz_size2index_usize_fastpath
JE_ALWAYS_INLINE void sizeToIndexUsizeFastpath(size_t size, szind_t * ind, size_t * usize)
{
    if (__builtin_constant_p(size))
    {
        /// When inlined, the size may become known at compile time, which allows static computation.
        *ind = sizeToIndexCompute(size);
        JE_ASSERT(*ind == sizeToIndexLookupImpl(size));
        *usize = indexToSizeCompute(*ind);
        JE_ASSERT(*usize == indexToSizeLookupImpl(*ind));
    }
    else
    {
        *ind = sizeToIndexLookupImpl(size);
        *usize = indexToSizeLookupImpl(*ind);
    }
}

/// jemalloc: sz_s2u_compute_using_delta
JE_ALWAYS_INLINE size_t s2uComputeUsingDelta(size_t size)
{
    size_t x = lgFloor((size << 1) - 1);
    size_t lg_delta = (x < SC_LG_NGROUP + LG_QUANTUM + 1) ? LG_QUANTUM : x - SC_LG_NGROUP - 1;
    size_t delta = size_t(1) << lg_delta;
    size_t delta_mask = delta - 1;
    size_t usize = (size + delta_mask) & ~delta_mask;
    return usize;
}

/// jemalloc: sz_s2u_compute. Returns 0 if the size is too large.
JE_ALWAYS_INLINE size_t s2uCompute(size_t size)
{
    if (JE_UNLIKELY(size > SC_LARGE_MAXCLASS))
        return 0;

    if (size == 0)
        ++size;

    if constexpr (SC_NTINY > 0)
    {
        if (size <= (size_t(1) << SC_LG_TINY_MAXCLASS))
        {
            size_t lg_tmin = SC_LG_TINY_MAXCLASS - SC_NTINY + 1;
            size_t lg_ceil = lgFloor(pow2Ceil(size));
            return (lg_ceil < lg_tmin ? (size_t(1) << lg_tmin) : (size_t(1) << lg_ceil));
        }
    }

    if (size <= SC_SMALL_MAXCLASS || !largeSizeClassesDisabled())
        return s2uComputeUsingDelta(size);

    /// With large size classes disabled, the usize of a large allocation is the size rounded up to a multiple of
    /// PAGE to minimize the memory overhead.
    size_t usize = pageCeiling(size);
    JE_ASSERT(usize - size < PAGE);
    return usize;
}

/// jemalloc: sz_s2u_lookup
JE_ALWAYS_INLINE size_t s2uLookup(size_t size)
{
    JE_ASSERT(size < SC_LARGE_MINCLASS);
    size_t ret = indexToSizeLookup(sizeToIndexLookup(size));
    JE_ASSERT(ret == s2uCompute(size));
    return ret;
}

/// Usable size that would result from allocating an object with the specified size; 0 if too large.
/// jemalloc: sz_s2u
JE_ALWAYS_INLINE size_t s2u(size_t size)
{
    if (JE_LIKELY(size <= SC_LOOKUP_MAXCLASS))
        return s2uLookup(size);
    return s2uCompute(size);
}

/// Usable size that would result from allocating an object with the specified size and alignment (a power of
/// two); 0 on overflow. The result is not checked against SC_LARGE_MAXCLASS. jemalloc: sz_sa2u
JE_ALWAYS_INLINE size_t sa2u(size_t size, size_t alignment)
{
    size_t usize;

    JE_ASSERT(alignment != 0 && ((alignment - 1) & alignment) == 0);

    /// Try for a small size class.
    if (size <= SC_SMALL_MAXCLASS && alignment <= PAGE)
    {
        /// Round size up to the nearest multiple of alignment. Every small size class object is aligned at the
        /// smallest power of two that is non-zero in the base two representation of the size.
        usize = s2u(alignmentCeiling(size, alignment));
        if (usize < SC_LARGE_MINCLASS)
            return usize;
    }

    /// Large size class. Beware of overflow.

    if (JE_UNLIKELY(alignment > SC_LARGE_MAXCLASS))
        return 0;

    /// Make sure result is a large size class.
    if (size <= SC_LARGE_MINCLASS)
        usize = SC_LARGE_MINCLASS;
    else
    {
        usize = s2u(size);
        if (usize < size)
        {
            /// size_t overflow.
            return 0;
        }
    }

    /// Calculate the multi-page mapping that large_palloc() would need in order to guarantee the alignment.
    if (usize + sz_large_pad + pageCeiling(alignment) - PAGE < usize)
    {
        /// size_t overflow.
        return 0;
    }
    return usize;
}

/// Whether an allocation of this (usable) size is served from a slab (unless it is sampled for profiling).
/// jemalloc: sz_can_use_slab
JE_ALWAYS_INLINE bool canUseSlab(size_t size)
{
    return size <= SC_SMALL_MAXCLASS;
}

/// jemalloc: sz_psz_quantize_floor. `size` is page aligned and > 0.
size_t pszQuantizeFloor(size_t size);

/// jemalloc: sz_psz_quantize_ceil. `size` is page aligned and > 0.
size_t pszQuantizeCeil(size_t size);

}

/// --- Region index division (div.h) ----------------------------------------------------------------------------

/// Computes the index of a region in a slab, given its offset relative to the slab base: for n = i * d, returns i
/// by multiplication with magic = ceil(2^32 / d). Requires n < 2^32 and d > 1.
/// jemalloc: div_info_t (without the `JEMALLOC_DEBUG`-only divisor field)
struct DivInfo
{
    uint32_t magic;

    /// jemalloc: div_init
    constexpr void init(size_t d)
    {
        /// Nonsensical.
        JE_ASSERT(d != 0);
        /// This would make the value of magic too high to fit into a uint32_t.
        JE_ASSERT(d != 1);

        uint64_t two_to_k = uint64_t(1) << 32;
        uint32_t m = uint32_t(two_to_k / d);

        /// We want magic = ceil(2^k / d), but C gives us floor; increment unless the result was exact.
        if (two_to_k % d != 0)
            ++m;
        magic = m;
    }

    /// jemalloc: div_compute
    JE_ALWAYS_INLINE size_t compute(size_t n) const
    {
        JE_ASSERT(n <= uint32_t(-1));
        return size_t((uint64_t(n) * uint64_t(magic)) >> 32);
    }
};

static_assert(sizeof(DivInfo) == 4);

/// --- Bin metadata (bin_info.h, bin_types.h) --------------------------------------------------------------------

/// jemalloc: BIN_SHARDS_MAX (1 << EDATA_BITS_BINSHARD_WIDTH), N_BIN_SHARDS_DEFAULT
inline constexpr unsigned BIN_SHARDS_MAX = 1u << 6;
inline constexpr unsigned N_BIN_SHARDS_DEFAULT = 1;

/// Read-only information associated with each small size class (shared by all arenas). A slab consists of `nregs`
/// regions of `reg_size` bytes back to back from its base, without a header.
/// jemalloc: bin_info_t
struct BinInfo
{
    /// Size of regions in a slab for this bin's size class.
    size_t reg_size;
    /// Total size of a slab for this bin's size class.
    size_t slab_size;
    /// Total number of regions in a slab for this bin's size class.
    uint32_t nregs;
    /// Number of sharded bins in each arena for this size class.
    uint32_t n_shards;
    /// Metadata used to manipulate bitmaps for slabs associated with this bin.
    BitmapInfo bitmap_info;
};

namespace detail
{

/// jemalloc: bin_infos_init
constexpr void binInfosInit(const SizeClassData & sc_data, const unsigned * bin_shard_sizes, BinInfo * infos)
{
    for (unsigned i = 0; i < SC_NBINS; ++i)
    {
        BinInfo & bin_info = infos[i];
        const SizeClass & sc = sc_data.sc[i];
        bin_info.reg_size = (size_t(1) << sc.lg_base) + (size_t(sc.ndelta) << sc.lg_delta);
        bin_info.slab_size = size_t(sc.pgs << LG_PAGE);
        bin_info.nregs = uint32_t(bin_info.slab_size / bin_info.reg_size);
        bin_info.n_shards = bin_shard_sizes[i];
        bin_info.bitmap_info = bitmapInfoInitializer(bin_info.nregs);
    }
}

consteval std::array<BinInfo, SC_NBINS> makeDefaultBinInfos()
{
    unsigned shards[SC_NBINS];
    for (auto & s : shards)
        s = N_BIN_SHARDS_DEFAULT;
    std::array<BinInfo, SC_NBINS> infos{};
    binInfosInit(default_sc_data, shards, infos.data());
    return infos;
}

}

/// The default bin metadata (before the `slab_sizes` and `bin_shards` options are applied).
inline constexpr std::array<BinInfo, SC_NBINS> default_bin_infos = detail::makeDefaultBinInfos();

/// jemalloc: bin_infos. Constant-initialized with the defaults; recomputed by `binInfoBoot`, read-only afterwards.
extern constinit std::array<BinInfo, SC_NBINS> bin_infos;

/// jemalloc: bin_info_boot
void binInfoBoot(const SizeClassData & sc_data, const unsigned * bin_shard_sizes);

/// Sets the number of shards of the small classes with sizes in [start_size, end_size].
/// Returns true on error (`nshards` is 0 or greater than BIN_SHARDS_MAX). jemalloc: bin_update_shard_size
bool binUpdateShardSize(unsigned * bin_shard_sizes, size_t start_size, size_t end_size, size_t nshards);

/// Loads the default number of shards. jemalloc: bin_shard_sizes_boot
void binShardSizesBoot(unsigned * bin_shard_sizes);

}
