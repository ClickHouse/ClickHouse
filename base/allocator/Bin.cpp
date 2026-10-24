#include <allocator/Bin.h>

#include <allocator/Arena.h>
#include <allocator/ThreadState.h>

namespace jemalloc
{

/// jemalloc: bin_init
bool Bin::init()
{
    if (lock.init("bin", MutexRank::BIN, MutexLockOrder::RankExclusive))
        return true;
    current_slab = nullptr;
    slabs_non_full.init();
    slabs_full.init();
    if constexpr (config::stats)
        stats = BinStats{};
    return false;
}

/// jemalloc: bin_slab_reg_alloc
void * Bin::slabRegionAlloc(Extent * slab, const BinInfo & bin_info)
{
    SlabData * slab_data = slab->slabData();

    ALLOCATOR_ASSERT(slab->numFree() > 0);
    ALLOCATOR_ASSERT(!bitmapFull(slab_data->bitmap, bin_info.bitmap_info));

    size_t region_idx = bitmapSetFirstUnset(slab_data->bitmap, bin_info.bitmap_info);
    void * result = static_cast<std::byte *>(slab->addr()) + uintptr_t(bin_info.region_size * region_idx);
    slab->numFreeDecrement();
    return result;
}

/// jemalloc: bin_slab_reg_alloc_batch
void Bin::slabRegionAllocBatch(Extent * slab, const BinInfo & bin_info, unsigned count, void ** ptrs)
{
    SlabData * slab_data = slab->slabData();

    ALLOCATOR_ASSERT(slab->numFree() >= count);
    ALLOCATOR_ASSERT(!bitmapFull(slab_data->bitmap, bin_info.bitmap_info));

    if constexpr (BITMAP_USE_TREE)
    {
        for (unsigned i = 0; i < count; ++i)
        {
            size_t region_idx = bitmapSetFirstUnset(slab_data->bitmap, bin_info.bitmap_info);
            ptrs[i] = reinterpret_cast<void *>(reinterpret_cast<uintptr_t>(slab->addr()) + uintptr_t(bin_info.region_size * region_idx));
        }
    }
    else
    {
        unsigned group = 0;
        bitmap_t g = slab_data->bitmap[group];
        unsigned i = 0;
        while (i < count)
        {
            while (g == 0)
                g = slab_data->bitmap[++group];
            size_t shift = size_t(group) << LOG2_BITMAP_GROUP_NUM_BITS;
            size_t pop = popcount(g);
            if (pop > (count - i))
                pop = count - i;

            /// Load from memory locations only once, outside the hot loop below.
            uintptr_t base = reinterpret_cast<uintptr_t>(slab->addr());
            uintptr_t region_size = uintptr_t(bin_info.region_size);
            while (pop--)
            {
                size_t bit = clearFirstSet(g);
                size_t region_idx = shift + bit;
                ptrs[i] = reinterpret_cast<void *>(base + region_size * region_idx);
                ++i;
            }
            slab_data->bitmap[group] = g;
        }
    }
    slab->numFreeSub(count);
}

/// jemalloc: bin_slabs_nonfull_insert
void Bin::slabsNonFullInsert(Extent * slab)
{
    ALLOCATOR_ASSERT(slab->numFree() > 0);
    slabs_non_full.insert(slab);
    if constexpr (config::stats)
        ++stats.non_full_slabs;
}

/// jemalloc: bin_slabs_nonfull_remove
void Bin::slabsNonFullRemove(Extent * slab)
{
    slabs_non_full.remove(slab);
    if constexpr (config::stats)
        --stats.non_full_slabs;
}

/// jemalloc: bin_slabs_nonfull_tryget
Extent * Bin::slabsNonFullTryGet()
{
    Extent * slab = slabs_non_full.removeFirst();
    if (slab == nullptr)
        return nullptr;
    if constexpr (config::stats)
    {
        ++stats.slab_changes;
        --stats.non_full_slabs;
    }
    return slab;
}

/// jemalloc: bin_slabs_full_insert
void Bin::slabsFullInsert(bool is_auto, Extent * slab)
{
    ALLOCATOR_ASSERT(slab->numFree() == 0);
    if (is_auto)
        return;
    slabs_full.append(slab);
}

/// jemalloc: bin_slabs_full_remove
void Bin::slabsFullRemove(bool is_auto, Extent * slab)
{
    if (is_auto)
        return;
    slabs_full.remove(slab);
}

/// jemalloc: bin_dissociate_slab
void Bin::dissociateSlab(bool is_auto, Extent * slab)
{
    /// Dissociate slab from bin.
    if (slab == current_slab)
    {
        current_slab = nullptr;
    }
    else
    {
        SizeClassIdx bin_idx = slab->sizeClassIdx();
        const BinInfo & bin_info = bin_infos[bin_idx];

        /// The following block's conditional is necessary because if the slab only contains one region, then it
        /// never gets inserted into the non-full slabs heap.
        if (bin_info.num_regions == 1)
            slabsFullRemove(is_auto, slab);
        else
            slabsNonFullRemove(slab);
    }
}

/// jemalloc: bin_lower_slab
void Bin::lowerSlab(ThreadState * /*tsdn*/, bool is_auto, Extent * slab)
{
    ALLOCATOR_ASSERT(slab->numFree() > 0);

    if (current_slab != nullptr && Extent::compareSerialNumberAndAddress(current_slab, slab) > 0)
    {
        /// Switch slabcur.
        if (current_slab->numFree() > 0)
            slabsNonFullInsert(current_slab);
        else
            slabsFullInsert(is_auto, current_slab);
        current_slab = slab;
        if constexpr (config::stats)
            ++stats.slab_changes;
    }
    else
    {
        slabsNonFullInsert(slab);
    }
}

/// jemalloc: bin_dalloc_slab_prepare
void Bin::deallocateSlabPrepare(ThreadState * thread_state, [[maybe_unused]] Extent * slab)
{
    lock.assertOwner(thread_state);

    ALLOCATOR_ASSERT(slab != current_slab);
    if constexpr (config::stats)
        --stats.current_slabs;
}

/// jemalloc: bin_dalloc_locked_handle_newly_empty
void Bin::deallocateLockedHandleNewlyEmpty(ThreadState * thread_state, bool is_auto, Extent * slab)
{
    dissociateSlab(is_auto, slab);
    deallocateSlabPrepare(thread_state, slab);
}

/// jemalloc: bin_dalloc_locked_handle_newly_nonempty
void Bin::deallocateLockedHandleNewlyNonempty(ThreadState * thread_state, bool is_auto, Extent * slab)
{
    slabsFullRemove(is_auto, slab);
    lowerSlab(thread_state, is_auto, slab);
}

/// jemalloc: bin_refill_slabcur_with_fresh_slab
void Bin::refillCurrentSlabWithFreshSlab(ThreadState * thread_state, [[maybe_unused]] SizeClassIdx bin_idx, Extent * fresh_slab)
{
    lock.assertOwner(thread_state);
    /// Only called after slabcur and nonfull both failed.
    ALLOCATOR_ASSERT(current_slab == nullptr);
    ALLOCATOR_ASSERT(slabs_non_full.first() == nullptr);
    ALLOCATOR_ASSERT(fresh_slab != nullptr);

    /// A new slab from `arenaSlabAlloc`.
    ALLOCATOR_ASSERT(fresh_slab->numFree() == bin_infos[bin_idx].num_regions);
    if constexpr (config::stats)
    {
        ++stats.num_slabs;
        ++stats.current_slabs;
    }
    current_slab = fresh_slab;
}

/// jemalloc: bin_malloc_with_fresh_slab
void * Bin::mallocWithFreshSlab(ThreadState * thread_state, SizeClassIdx bin_idx, Extent * fresh_slab)
{
    lock.assertOwner(thread_state);
    refillCurrentSlabWithFreshSlab(thread_state, bin_idx, fresh_slab);

    return slabRegionAlloc(current_slab, bin_infos[bin_idx]);
}

/// jemalloc: bin_refill_slabcur_no_fresh_slab
bool Bin::refillCurrentSlabNoFreshSlab(ThreadState * thread_state, bool is_auto)
{
    lock.assertOwner(thread_state);
    /// Only called after `slabRegionAlloc[Batch]` failed.
    ALLOCATOR_ASSERT(current_slab == nullptr || current_slab->numFree() == 0);

    if (current_slab != nullptr)
        slabsFullInsert(is_auto, current_slab);

    /// Look for a usable slab.
    current_slab = slabsNonFullTryGet();
    ALLOCATOR_ASSERT(current_slab == nullptr || current_slab->numFree() > 0);

    return current_slab == nullptr;
}

/// jemalloc: bin_malloc_no_fresh_slab
void * Bin::mallocNoFreshSlab(ThreadState * thread_state, bool is_auto, SizeClassIdx bin_idx)
{
    lock.assertOwner(thread_state);
    if (current_slab == nullptr || current_slab->numFree() == 0)
    {
        if (refillCurrentSlabNoFreshSlab(thread_state, is_auto))
            return nullptr;
    }

    ALLOCATOR_ASSERT(current_slab != nullptr && current_slab->numFree() > 0);
    return slabRegionAlloc(current_slab, bin_infos[bin_idx]);
}

/// jemalloc: bin_stats_merge
void Bin::statsMerge(ThreadState * thread_state, BinStatsData & dst_bin_stats)
{
    lock.lock(thread_state);
    lock.profilingAccumulated(thread_state, dst_bin_stats.mutex_data);
    BinStats & dst = dst_bin_stats.stats_data;
    dst.num_allocations += stats.num_allocations;
    dst.num_deallocations += stats.num_deallocations;
    dst.num_requests += stats.num_requests;
    dst.current_regions += stats.current_regions;
    dst.num_fills += stats.num_fills;
    dst.num_flushes += stats.num_flushes;
    dst.num_slabs += stats.num_slabs;
    dst.slab_changes += stats.slab_changes;
    dst.current_slabs += stats.current_slabs;
    dst.non_full_slabs += stats.non_full_slabs;
    lock.unlock(thread_state);
}

/// jemalloc: bin_choose
Bin * binChoose(ThreadState * thread_state, Arena * arena, SizeClassIdx bin_idx, unsigned * bin_shard_ptr)
{
    unsigned bin_shard;
    if (thread_state == nullptr || thread_state->arena == nullptr)
        bin_shard = 0;
    else
        bin_shard = thread_state->bin_shards.bin_shard[bin_idx];
    ALLOCATOR_ASSERT(bin_shard < bin_infos[bin_idx].num_shards);
    if (bin_shard_ptr != nullptr)
        *bin_shard_ptr = bin_shard;
    return arenaGetBin(arena, bin_idx, bin_shard);
}

}
