#include <allocator/ExtentSet.h>

namespace jemalloc
{

void ExtentSet::init(ExtentState state_)
{
    for (unsigned i = 0; i < ESET_NPSIZES; ++i)
    {
        /// jemalloc: eset_bin_init. `heap_min` doesn't need initialization; it gets filled in when the bin goes from
        /// empty to non-empty.
        bins[i].heap.init();
        /// jemalloc: eset_bin_stats_init
        bin_stats[i].nextents.store(0, std::memory_order_relaxed);
        bin_stats[i].nbytes.store(0, std::memory_order_relaxed);
    }
    bitmap.init();
    lru.init();
    state = state_;
}

void ExtentSet::insert(Extent * edata)
{
    JE_ASSERT(edata->state() == state);

    size_t size = edata->size();
    size_t psz = sz::pszQuantizeFloor(size);
    pszind_t pind = sz::psz2ind(psz);

    ExtentCmpSummary edata_cmp_summary = edata->cmpSummary();
    if (bins[pind].heap.empty())
    {
        bitmap.set(pind);
        /// Only element is automatically the min element.
        bins[pind].heap_min = edata_cmp_summary;
    }
    else
    {
        /// There's already a min element; update the summary if we're about to insert a lower one.
        if (Extent::compareSummary(edata_cmp_summary, bins[pind].heap_min) < 0)
            bins[pind].heap_min = edata_cmp_summary;
    }
    bins[pind].heap.insert(edata);

    if constexpr (config::stats)
        statsAdd(pind, size);

    lru.append(edata);
    size_t npages_add = size >> LG_PAGE;
    /// All modifications to npages hold the mutex, so we don't need an atomic fetch-add; we can get by with a load
    /// followed by a store.
    size_t cur_eset_npages = npages.load(std::memory_order_relaxed);
    npages.store(cur_eset_npages + npages_add, std::memory_order_relaxed);
}

void ExtentSet::remove(Extent * edata)
{
    JE_ASSERT(edata->state() == state || extentStateInTransition(edata->state()));

    size_t size = edata->size();
    size_t psz = sz::pszQuantizeFloor(size);
    pszind_t pind = sz::psz2ind(psz);
    if constexpr (config::stats)
        statsSub(pind, size);

    ExtentCmpSummary edata_cmp_summary = edata->cmpSummary();
    bins[pind].heap.remove(edata);
    if (bins[pind].heap.empty())
    {
        bitmap.unset(pind);
    }
    else
    {
        /// Compare whether the summaries are equal, rather than whether the removed extent was the heap minimum:
        /// getting the heap minimum can cause a pairing heap merge operation. We can avoid this if we only update the
        /// min if it's changed, in which case the summaries of the removed element and the min element compare equal.
        if (Extent::compareSummary(edata_cmp_summary, bins[pind].heap_min) == 0)
            bins[pind].heap_min = bins[pind].heap.first()->cmpSummary();
    }
    lru.remove(edata);
    /// As in `insert`, we hold the mutex and so don't need atomic operations for updating npages.
    size_t cur_extents_npages = npages.load(std::memory_order_relaxed);
    JE_ASSERT(cur_extents_npages >= (size >> LG_PAGE));
    npages.store(cur_extents_npages - (size >> LG_PAGE), std::memory_order_relaxed);
}

Extent * ExtentSet::enumerateAlignmentSearch(size_t size, pszind_t bin_ind, size_t alignment)
{
    if (bins[bin_ind].heap.empty())
        return nullptr;

    Extent * edata = nullptr;
    ExtentHeapEnumerateHelper helper;
    bins[bin_ind].heap.enumeratePrepare(helper, ESET_ENUMERATE_MAX_NUM, sizeof(helper.bfs_queue) / sizeof(void *));
    while ((edata = bins[bin_ind].heap.enumerateNext(helper)) != nullptr)
    {
        uintptr_t base = reinterpret_cast<uintptr_t>(edata->base());
        size_t candidate_size = edata->size();
        if (candidate_size < size)
            continue;

        uintptr_t next_align = alignmentCeiling(base, pageCeiling(alignment));
        if (base > next_align || base + candidate_size <= next_align)
        {
            /// Overflow or not crossing the next alignment.
            continue;
        }

        size_t leadsize = next_align - base;
        if (candidate_size - leadsize >= size)
            return edata;
    }

    return nullptr;
}

Extent * ExtentSet::enumerateSearch(size_t size, pszind_t bin_ind, bool exact_only, ExtentCmpSummary * ret_summ)
{
    if (bins[bin_ind].heap.empty())
        return nullptr;

    Extent * ret = nullptr;
    Extent * edata = nullptr;
    ExtentHeapEnumerateHelper helper;
    bins[bin_ind].heap.enumeratePrepare(helper, ESET_ENUMERATE_MAX_NUM, sizeof(helper.bfs_queue) / sizeof(void *));
    while ((edata = bins[bin_ind].heap.enumerateNext(helper)) != nullptr)
    {
        if ((!exact_only && edata->size() >= size) || (exact_only && edata->size() == size))
        {
            ExtentCmpSummary temp_summ = edata->cmpSummary();
            if (ret == nullptr || Extent::compareSummary(temp_summ, *ret_summ) < 0)
            {
                ret = edata;
                *ret_summ = temp_summ;
            }
        }
    }

    return ret;
}

Extent * ExtentSet::fitAlignment(size_t min_size, size_t max_size, size_t alignment)
{
    pszind_t pind = sz::psz2ind(sz::pszQuantizeCeil(min_size));
    pszind_t pind_max = sz::psz2ind(sz::pszQuantizeCeil(max_size));

    /// See the comments in `firstFit` for why we enumerate search below.
    pszind_t pind_prev = sz::psz2ind(sz::pszQuantizeFloor(min_size));
    if (sz::largeSizeClassesDisabled() && pind != pind_prev)
    {
        Extent * ret = enumerateAlignmentSearch(min_size, pind_prev, alignment);
        if (ret != nullptr)
            return ret;
    }

    for (pszind_t i = pszind_t(bitmap.ffs(size_t(pind))); i < pind_max; i = pszind_t(bitmap.ffs(size_t(i) + 1)))
    {
        JE_ASSERT(i < SC_NPSIZES);
        JE_ASSERT(!bins[i].heap.empty());
        Extent * edata = bins[i].heap.first();
        uintptr_t base = reinterpret_cast<uintptr_t>(edata->base());
        size_t candidate_size = edata->size();
        JE_ASSERT(candidate_size >= min_size);

        uintptr_t next_align = alignmentCeiling(base, pageCeiling(alignment));
        if (base > next_align || base + candidate_size <= next_align)
        {
            /// Overflow or not crossing the next alignment.
            continue;
        }

        size_t leadsize = next_align - base;
        if (candidate_size - leadsize >= min_size)
            return edata;
    }

    return nullptr;
}

/// `lg_max_fit` is the (log of the) maximum ratio between the requested size and the returned size that we'll allow.
/// This can reduce fragmentation by avoiding reusing and splitting large extents for smaller sizes. In practice, it's
/// set to `opt.lg_extent_max_active_fit` for the dirty set and `SC_PTR_BITS` for others.
Extent * ExtentSet::firstFit(size_t size, bool exact_only, unsigned lg_max_fit)
{
    Extent * ret = nullptr;
    ExtentCmpSummary ret_summ{0, 0};

    pszind_t pind = sz::psz2ind(sz::pszQuantizeCeil(size));

    if (exact_only)
    {
        if (sz::largeSizeClassesDisabled())
        {
            pszind_t pind_prev = sz::psz2ind(sz::pszQuantizeFloor(size));
            return enumerateSearch(size, pind_prev, /* exact_only */ true, &ret_summ);
        }
        else
        {
            return bins[pind].heap.empty() ? nullptr : bins[pind].heap.first();
        }
    }

    /// Each element in `bins` is a heap corresponding to a size class. When large size classes are not disabled, all
    /// heaps after `pind` (including `pind` itself) will surely satisfy the request while heaps before `pind` cannot,
    /// because usize is calculated based on size classes then. However, when large size classes are disabled, usize
    /// is calculated by ceiling the requested size to the closest multiple of PAGE. This means that the heap before
    /// `pind`, i.e. `pind_prev`, may contain extents able to satisfy the request, and we should enumerate it when
    /// `pind_prev != pind`.
    ///
    /// For example, when PAGE = 4KB and the requested size is 1MB + 4KB, usize would be 1.25MB with large size
    /// classes. `pind` points to the heap containing extents in [1.25MB, 1.5MB). Thus, searching starting from `pind`
    /// will not miss any candidates. With large size classes disabled, usize would be 1MB + 4KB and `pind` still
    /// points to the same heap. In this case, the heap `pind_prev` points to, which contains extents in
    /// [1MB, 1.25MB), may contain candidates satisfying the usize and thus should be enumerated.
    pszind_t pind_prev = sz::psz2ind(sz::pszQuantizeFloor(size));
    if (sz::largeSizeClassesDisabled() && pind != pind_prev)
        ret = enumerateSearch(size, pind_prev, /* exact_only */ false, &ret_summ);

    for (pszind_t i = pszind_t(bitmap.ffs(size_t(pind))); i < ESET_NPSIZES; i = pszind_t(bitmap.ffs(size_t(i) + 1)))
    {
        JE_ASSERT(!bins[i].heap.empty());
        if (lg_max_fit == SC_PTR_BITS)
        {
            /// We'll shift by this below, and shifting out all the bits is undefined. Decreasing is safe, since the
            /// page size is larger than 1 byte.
            lg_max_fit = SC_PTR_BITS - 1;
        }
        if ((sz::pind2sz(i) >> lg_max_fit) > size)
            break;
        if (ret == nullptr || Extent::compareSummary(bins[i].heap_min, ret_summ) < 0)
        {
            /// We grab the extent as early as possible, even though we might change it later. Practically, a large
            /// portion of `fit` calls succeed at the first valid index, so this doesn't cost much, and we get the
            /// effect of prefetching the extent as early as possible.
            Extent * edata = bins[i].heap.first();
            JE_ASSERT(edata->size() >= size);
            JE_ASSERT(ret == nullptr || Extent::compareSnad(edata, ret) < 0);
            JE_ASSERT(ret == nullptr || Extent::compareSummary(bins[i].heap_min, edata->cmpSummary()) == 0);
            ret = edata;
            ret_summ = bins[i].heap_min;
        }
        if (i == SC_NPSIZES)
            break;
        JE_ASSERT(i < SC_NPSIZES);
    }

    return ret;
}

Extent * ExtentSet::fit(size_t esize, size_t alignment, bool exact_only, unsigned lg_max_fit)
{
    size_t max_size = esize + pageCeiling(alignment) - PAGE;
    /// Beware size_t wrap-around.
    if (max_size < esize)
        return nullptr;

    Extent * edata = firstFit(max_size, exact_only, lg_max_fit);

    if (alignment > PAGE && edata == nullptr)
    {
        /// `max_size` guarantees the alignment requirement but is rather pessimistic. Next we try to satisfy the
        /// aligned allocation with sizes in [esize, max_size).
        edata = fitAlignment(esize, max_size, alignment);
    }

    return edata;
}

}
