#include <allocator/ExtentSet.h>

namespace jemalloc
{

void ExtentSet::init(ExtentState state_)
{
    for (unsigned i = 0; i < EXTENT_SET_NUM_PAGE_SIZES; ++i)
    {
        /// jemalloc: eset_bin_init. `heap_min` doesn't need initialization; it gets filled in when the bin goes from
        /// empty to non-empty.
        bins[i].heap.init();
        /// jemalloc: eset_bin_stats_init
        bin_stats[i].num_extents.store(0, std::memory_order_relaxed);
        bin_stats[i].num_bytes.store(0, std::memory_order_relaxed);
    }
    bitmap.init();
    lru.init();
    state = state_;
}

void ExtentSet::insert(Extent * extent)
{
    ALLOCATOR_ASSERT(extent->state() == state);

    size_t size = extent->size();
    size_t page_size = size_classes::pageSizeQuantizeFloor(size);
    PageSizeClassIdx page_size_class_idx = size_classes::pageSizeToPageSizeClassIdx(page_size);

    ExtentComparisonSummary extent_comparison_summary = extent->comparisonSummary();
    if (bins[page_size_class_idx].heap.empty())
    {
        bitmap.set(page_size_class_idx);
        /// Only element is automatically the min element.
        bins[page_size_class_idx].heap_min = extent_comparison_summary;
    }
    else
    {
        /// There's already a min element; update the summary if we're about to insert a lower one.
        if (Extent::compareSummary(extent_comparison_summary, bins[page_size_class_idx].heap_min) < 0)
            bins[page_size_class_idx].heap_min = extent_comparison_summary;
    }
    bins[page_size_class_idx].heap.insert(extent);

    if constexpr (config::stats)
        statsAdd(page_size_class_idx, size);

    lru.append(extent);
    size_t num_pages_add = size >> LOG2_PAGE;
    /// All modifications to npages hold the mutex, so we don't need an atomic fetch-add; we can get by with a load
    /// followed by a store.
    size_t current_extent_set_num_pages = num_pages.load(std::memory_order_relaxed);
    num_pages.store(current_extent_set_num_pages + num_pages_add, std::memory_order_relaxed);
}

void ExtentSet::remove(Extent * extent)
{
    ALLOCATOR_ASSERT(extent->state() == state || extentStateInTransition(extent->state()));

    size_t size = extent->size();
    size_t page_size = size_classes::pageSizeQuantizeFloor(size);
    PageSizeClassIdx page_size_class_idx = size_classes::pageSizeToPageSizeClassIdx(page_size);
    if constexpr (config::stats)
        statsSub(page_size_class_idx, size);

    ExtentComparisonSummary extent_comparison_summary = extent->comparisonSummary();
    bins[page_size_class_idx].heap.remove(extent);
    if (bins[page_size_class_idx].heap.empty())
    {
        bitmap.unset(page_size_class_idx);
    }
    else
    {
        /// Compare whether the summaries are equal, rather than whether the removed extent was the heap minimum:
        /// getting the heap minimum can cause a pairing heap merge operation. We can avoid this if we only update the
        /// min if it's changed, in which case the summaries of the removed element and the min element compare equal.
        if (Extent::compareSummary(extent_comparison_summary, bins[page_size_class_idx].heap_min) == 0)
            bins[page_size_class_idx].heap_min = bins[page_size_class_idx].heap.first()->comparisonSummary();
    }
    lru.remove(extent);
    /// As in `insert`, we hold the mutex and so don't need atomic operations for updating npages.
    size_t current_extents_num_pages = num_pages.load(std::memory_order_relaxed);
    ALLOCATOR_ASSERT(current_extents_num_pages >= (size >> LOG2_PAGE));
    num_pages.store(current_extents_num_pages - (size >> LOG2_PAGE), std::memory_order_relaxed);
}

Extent * ExtentSet::enumerateAlignmentSearch(size_t size, PageSizeClassIdx bin_idx, size_t alignment)
{
    if (bins[bin_idx].heap.empty())
        return nullptr;

    Extent * extent = nullptr;
    ExtentHeapEnumerateHelper helper;
    bins[bin_idx].heap.enumeratePrepare(helper, EXTENT_SET_ENUMERATE_MAX_NUM, sizeof(helper.bfs_queue) / sizeof(void *));
    while ((extent = bins[bin_idx].heap.enumerateNext(helper)) != nullptr)
    {
        uintptr_t base = reinterpret_cast<uintptr_t>(extent->base());
        size_t candidate_size = extent->size();
        if (candidate_size < size)
            continue;

        uintptr_t next_align = alignmentCeiling(base, pageCeiling(alignment));
        if (base > next_align || base + candidate_size <= next_align)
        {
            /// Overflow or not crossing the next alignment.
            continue;
        }

        size_t lead_size = next_align - base;
        if (candidate_size - lead_size >= size)
            return extent;
    }

    return nullptr;
}

Extent * ExtentSet::enumerateSearch(size_t size, PageSizeClassIdx bin_idx, bool exact_only, ExtentComparisonSummary * result_summary)
{
    if (bins[bin_idx].heap.empty())
        return nullptr;

    Extent * result = nullptr;
    Extent * extent = nullptr;
    ExtentHeapEnumerateHelper helper;
    bins[bin_idx].heap.enumeratePrepare(helper, EXTENT_SET_ENUMERATE_MAX_NUM, sizeof(helper.bfs_queue) / sizeof(void *));
    while ((extent = bins[bin_idx].heap.enumerateNext(helper)) != nullptr)
    {
        if ((!exact_only && extent->size() >= size) || (exact_only && extent->size() == size))
        {
            ExtentComparisonSummary temp_summary = extent->comparisonSummary();
            if (result == nullptr || Extent::compareSummary(temp_summary, *result_summary) < 0)
            {
                result = extent;
                *result_summary = temp_summary;
            }
        }
    }

    return result;
}

Extent * ExtentSet::fitAlignment(size_t min_size, size_t max_size, size_t alignment)
{
    PageSizeClassIdx page_size_class_idx = size_classes::pageSizeToPageSizeClassIdx(size_classes::pageSizeQuantizeCeil(min_size));
    PageSizeClassIdx page_size_class_idx_max = size_classes::pageSizeToPageSizeClassIdx(size_classes::pageSizeQuantizeCeil(max_size));

    /// See the comments in `firstFit` for why we enumerate search below.
    PageSizeClassIdx page_size_class_idx_prev = size_classes::pageSizeToPageSizeClassIdx(size_classes::pageSizeQuantizeFloor(min_size));
    if (size_classes::largeSizeClassesDisabled() && page_size_class_idx != page_size_class_idx_prev)
    {
        Extent * result = enumerateAlignmentSearch(min_size, page_size_class_idx_prev, alignment);
        if (result != nullptr)
            return result;
    }

    for (PageSizeClassIdx i = PageSizeClassIdx(bitmap.findFirstSet(size_t(page_size_class_idx))); i < page_size_class_idx_max;
         i = PageSizeClassIdx(bitmap.findFirstSet(size_t(i) + 1)))
    {
        ALLOCATOR_ASSERT(i < SIZE_CLASS_NUM_PAGE_SIZES);
        ALLOCATOR_ASSERT(!bins[i].heap.empty());
        Extent * extent = bins[i].heap.first();
        uintptr_t base = reinterpret_cast<uintptr_t>(extent->base());
        size_t candidate_size = extent->size();
        ALLOCATOR_ASSERT(candidate_size >= min_size);

        uintptr_t next_align = alignmentCeiling(base, pageCeiling(alignment));
        if (base > next_align || base + candidate_size <= next_align)
        {
            /// Overflow or not crossing the next alignment.
            continue;
        }

        size_t lead_size = next_align - base;
        if (candidate_size - lead_size >= min_size)
            return extent;
    }

    return nullptr;
}

/// `log2_max_fit` is the (log of the) maximum ratio between the requested size and the returned size that we'll allow.
/// This can reduce fragmentation by avoiding reusing and splitting large extents for smaller sizes. In practice, it's
/// set to `opt.lg_extent_max_active_fit` for the dirty set and `SIZE_CLASS_PTR_BITS` for others.
Extent * ExtentSet::firstFit(size_t size, bool exact_only, unsigned log2_max_fit)
{
    Extent * result = nullptr;
    ExtentComparisonSummary result_summary{0, 0};

    PageSizeClassIdx page_size_class_idx = size_classes::pageSizeToPageSizeClassIdx(size_classes::pageSizeQuantizeCeil(size));

    if (exact_only)
    {
        if (size_classes::largeSizeClassesDisabled())
        {
            PageSizeClassIdx page_size_class_idx_prev = size_classes::pageSizeToPageSizeClassIdx(size_classes::pageSizeQuantizeFloor(size));
            return enumerateSearch(size, page_size_class_idx_prev, /* exact_only */ true, &result_summary);
        }
        else
        {
            return bins[page_size_class_idx].heap.empty() ? nullptr : bins[page_size_class_idx].heap.first();
        }
    }

    /// Each element in `bins` is a heap corresponding to a size class. When large size classes are not disabled, all
    /// heaps after `page_size_class_idx` (including `page_size_class_idx` itself) will surely satisfy the request while heaps before
    /// `page_size_class_idx` cannot,
    /// because usize is calculated based on size classes then. However, when large size classes are disabled, usize
    /// is calculated by ceiling the requested size to the closest multiple of PAGE. This means that the heap before
    /// `page_size_class_idx`, i.e. `page_size_class_idx_prev`, may contain extents able to satisfy the request, and we should enumerate it
    /// when
    /// `page_size_class_idx_prev != page_size_class_idx`.
    ///
    /// For example, when PAGE = 4KB and the requested size is 1MB + 4KB, usize would be 1.25MB with large size
    /// classes. `page_size_class_idx` points to the heap containing extents in [1.25MB, 1.5MB). Thus, searching starting from
    /// `page_size_class_idx`
    /// will not miss any candidates. With large size classes disabled, usize would be 1MB + 4KB and `page_size_class_idx` still
    /// points to the same heap. In this case, the heap `page_size_class_idx_prev` points to, which contains extents in
    /// [1MB, 1.25MB), may contain candidates satisfying the usize and thus should be enumerated.
    PageSizeClassIdx page_size_class_idx_prev = size_classes::pageSizeToPageSizeClassIdx(size_classes::pageSizeQuantizeFloor(size));
    if (size_classes::largeSizeClassesDisabled() && page_size_class_idx != page_size_class_idx_prev)
        result = enumerateSearch(size, page_size_class_idx_prev, /* exact_only */ false, &result_summary);

    for (PageSizeClassIdx i = PageSizeClassIdx(bitmap.findFirstSet(size_t(page_size_class_idx))); i < EXTENT_SET_NUM_PAGE_SIZES;
         i = PageSizeClassIdx(bitmap.findFirstSet(size_t(i) + 1)))
    {
        ALLOCATOR_ASSERT(!bins[i].heap.empty());
        if (log2_max_fit == SIZE_CLASS_PTR_BITS)
        {
            /// We'll shift by this below, and shifting out all the bits is undefined. Decreasing is safe, since the
            /// page size is larger than 1 byte.
            log2_max_fit = SIZE_CLASS_PTR_BITS - 1;
        }
        if ((size_classes::pageSizeClassIdxToSize(i) >> log2_max_fit) > size)
            break;
        if (result == nullptr || Extent::compareSummary(bins[i].heap_min, result_summary) < 0)
        {
            /// We grab the extent as early as possible, even though we might change it later. Practically, a large
            /// portion of `fit` calls succeed at the first valid index, so this doesn't cost much, and we get the
            /// effect of prefetching the extent as early as possible.
            Extent * extent = bins[i].heap.first();
            ALLOCATOR_ASSERT(extent->size() >= size);
            ALLOCATOR_ASSERT(result == nullptr || Extent::compareSerialNumberAndAddress(extent, result) < 0);
            ALLOCATOR_ASSERT(result == nullptr || Extent::compareSummary(bins[i].heap_min, extent->comparisonSummary()) == 0);
            result = extent;
            result_summary = bins[i].heap_min;
        }
        if (i == SIZE_CLASS_NUM_PAGE_SIZES)
            break;
        ALLOCATOR_ASSERT(i < SIZE_CLASS_NUM_PAGE_SIZES);
    }

    return result;
}

Extent * ExtentSet::fit(size_t extent_size, size_t alignment, bool exact_only, unsigned log2_max_fit)
{
    size_t max_size = extent_size + pageCeiling(alignment) - PAGE;
    /// Beware size_t wrap-around.
    if (max_size < extent_size)
        return nullptr;

    Extent * extent = firstFit(max_size, exact_only, log2_max_fit);

    if (alignment > PAGE && extent == nullptr)
    {
        /// `max_size` guarantees the alignment requirement but is rather pessimistic. Next we try to satisfy the
        /// aligned allocation with sizes in [esize, max_size).
        extent = fitAlignment(extent_size, max_size, alignment);
    }

    return extent;
}

}
