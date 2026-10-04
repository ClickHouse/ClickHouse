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
    slabcur = nullptr;
    slabs_nonfull.init();
    slabs_full.init();
    if constexpr (config::stats)
        stats = BinStats{};
    return false;
}

/// jemalloc: bin_slab_reg_alloc
void * Bin::slabRegAlloc(Extent * slab, const BinInfo & bin_info)
{
    SlabData * slab_data = slab->slabData();

    JE_ASSERT(slab->nfree() > 0);
    JE_ASSERT(!bitmapFull(slab_data->bitmap, bin_info.bitmap_info));

    size_t regind = bitmapSfu(slab_data->bitmap, bin_info.bitmap_info);
    void * ret = static_cast<std::byte *>(slab->addr()) + uintptr_t(bin_info.reg_size * regind);
    slab->nfreeDec();
    return ret;
}

/// jemalloc: bin_slab_reg_alloc_batch
void Bin::slabRegAllocBatch(Extent * slab, const BinInfo & bin_info, unsigned cnt, void ** ptrs)
{
    SlabData * slab_data = slab->slabData();

    JE_ASSERT(slab->nfree() >= cnt);
    JE_ASSERT(!bitmapFull(slab_data->bitmap, bin_info.bitmap_info));

    if constexpr (BITMAP_USE_TREE)
    {
        for (unsigned i = 0; i < cnt; ++i)
        {
            size_t regind = bitmapSfu(slab_data->bitmap, bin_info.bitmap_info);
            ptrs[i] = reinterpret_cast<void *>(reinterpret_cast<uintptr_t>(slab->addr()) + uintptr_t(bin_info.reg_size * regind));
        }
    }
    else
    {
        unsigned group = 0;
        bitmap_t g = slab_data->bitmap[group];
        unsigned i = 0;
        while (i < cnt)
        {
            while (g == 0)
                g = slab_data->bitmap[++group];
            size_t shift = size_t(group) << LG_BITMAP_GROUP_NBITS;
            size_t pop = popcount(g);
            if (pop > (cnt - i))
                pop = cnt - i;

            /// Load from memory locations only once, outside the hot loop below.
            uintptr_t base = reinterpret_cast<uintptr_t>(slab->addr());
            uintptr_t regsize = uintptr_t(bin_info.reg_size);
            while (pop--)
            {
                size_t bit = cfs(g);
                size_t regind = shift + bit;
                ptrs[i] = reinterpret_cast<void *>(base + regsize * regind);
                ++i;
            }
            slab_data->bitmap[group] = g;
        }
    }
    slab->nfreeSub(cnt);
}

/// jemalloc: bin_slabs_nonfull_insert
void Bin::slabsNonfullInsert(Extent * slab)
{
    JE_ASSERT(slab->nfree() > 0);
    slabs_nonfull.insert(slab);
    if constexpr (config::stats)
        ++stats.nonfull_slabs;
}

/// jemalloc: bin_slabs_nonfull_remove
void Bin::slabsNonfullRemove(Extent * slab)
{
    slabs_nonfull.remove(slab);
    if constexpr (config::stats)
        --stats.nonfull_slabs;
}

/// jemalloc: bin_slabs_nonfull_tryget
Extent * Bin::slabsNonfullTryget()
{
    Extent * slab = slabs_nonfull.removeFirst();
    if (slab == nullptr)
        return nullptr;
    if constexpr (config::stats)
    {
        ++stats.reslabs;
        --stats.nonfull_slabs;
    }
    return slab;
}

/// jemalloc: bin_slabs_full_insert
void Bin::slabsFullInsert(bool is_auto, Extent * slab)
{
    JE_ASSERT(slab->nfree() == 0);
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
    if (slab == slabcur)
    {
        slabcur = nullptr;
    }
    else
    {
        szind_t binind = slab->szind();
        const BinInfo & bin_info = bin_infos[binind];

        /// The following block's conditional is necessary because if the slab only contains one region, then it
        /// never gets inserted into the non-full slabs heap.
        if (bin_info.nregs == 1)
            slabsFullRemove(is_auto, slab);
        else
            slabsNonfullRemove(slab);
    }
}

/// jemalloc: bin_lower_slab
void Bin::lowerSlab(ThreadState * /*tsdn*/, bool is_auto, Extent * slab)
{
    JE_ASSERT(slab->nfree() > 0);

    if (slabcur != nullptr && Extent::compareSnad(slabcur, slab) > 0)
    {
        /// Switch slabcur.
        if (slabcur->nfree() > 0)
            slabsNonfullInsert(slabcur);
        else
            slabsFullInsert(is_auto, slabcur);
        slabcur = slab;
        if constexpr (config::stats)
            ++stats.reslabs;
    }
    else
    {
        slabsNonfullInsert(slab);
    }
}

/// jemalloc: bin_dalloc_slab_prepare
void Bin::dallocSlabPrepare(ThreadState * tsdn, [[maybe_unused]] Extent * slab)
{
    lock.assertOwner(tsdn);

    JE_ASSERT(slab != slabcur);
    if constexpr (config::stats)
        --stats.curslabs;
}

/// jemalloc: bin_dalloc_locked_handle_newly_empty
void Bin::dallocLockedHandleNewlyEmpty(ThreadState * tsdn, bool is_auto, Extent * slab)
{
    dissociateSlab(is_auto, slab);
    dallocSlabPrepare(tsdn, slab);
}

/// jemalloc: bin_dalloc_locked_handle_newly_nonempty
void Bin::dallocLockedHandleNewlyNonempty(ThreadState * tsdn, bool is_auto, Extent * slab)
{
    slabsFullRemove(is_auto, slab);
    lowerSlab(tsdn, is_auto, slab);
}

/// jemalloc: bin_refill_slabcur_with_fresh_slab
void Bin::refillSlabcurWithFreshSlab(ThreadState * tsdn, [[maybe_unused]] szind_t binind, Extent * fresh_slab)
{
    lock.assertOwner(tsdn);
    /// Only called after slabcur and nonfull both failed.
    JE_ASSERT(slabcur == nullptr);
    JE_ASSERT(slabs_nonfull.first() == nullptr);
    JE_ASSERT(fresh_slab != nullptr);

    /// A new slab from `arenaSlabAlloc`.
    JE_ASSERT(fresh_slab->nfree() == bin_infos[binind].nregs);
    if constexpr (config::stats)
    {
        ++stats.nslabs;
        ++stats.curslabs;
    }
    slabcur = fresh_slab;
}

/// jemalloc: bin_malloc_with_fresh_slab
void * Bin::mallocWithFreshSlab(ThreadState * tsdn, szind_t binind, Extent * fresh_slab)
{
    lock.assertOwner(tsdn);
    refillSlabcurWithFreshSlab(tsdn, binind, fresh_slab);

    return slabRegAlloc(slabcur, bin_infos[binind]);
}

/// jemalloc: bin_refill_slabcur_no_fresh_slab
bool Bin::refillSlabcurNoFreshSlab(ThreadState * tsdn, bool is_auto)
{
    lock.assertOwner(tsdn);
    /// Only called after `slabRegAlloc[Batch]` failed.
    JE_ASSERT(slabcur == nullptr || slabcur->nfree() == 0);

    if (slabcur != nullptr)
        slabsFullInsert(is_auto, slabcur);

    /// Look for a usable slab.
    slabcur = slabsNonfullTryget();
    JE_ASSERT(slabcur == nullptr || slabcur->nfree() > 0);

    return slabcur == nullptr;
}

/// jemalloc: bin_malloc_no_fresh_slab
void * Bin::mallocNoFreshSlab(ThreadState * tsdn, bool is_auto, szind_t binind)
{
    lock.assertOwner(tsdn);
    if (slabcur == nullptr || slabcur->nfree() == 0)
    {
        if (refillSlabcurNoFreshSlab(tsdn, is_auto))
            return nullptr;
    }

    JE_ASSERT(slabcur != nullptr && slabcur->nfree() > 0);
    return slabRegAlloc(slabcur, bin_infos[binind]);
}

/// jemalloc: bin_stats_merge
void Bin::statsMerge(ThreadState * tsdn, BinStatsData & dst_bin_stats)
{
    lock.lock(tsdn);
    lock.profAccum(tsdn, dst_bin_stats.mutex_data);
    BinStats & dst = dst_bin_stats.stats_data;
    dst.nmalloc += stats.nmalloc;
    dst.ndalloc += stats.ndalloc;
    dst.nrequests += stats.nrequests;
    dst.curregs += stats.curregs;
    dst.nfills += stats.nfills;
    dst.nflushes += stats.nflushes;
    dst.nslabs += stats.nslabs;
    dst.reslabs += stats.reslabs;
    dst.curslabs += stats.curslabs;
    dst.nonfull_slabs += stats.nonfull_slabs;
    lock.unlock(tsdn);
}

/// jemalloc: bin_choose
Bin * binChoose(ThreadState * tsdn, Arena * arena, szind_t binind, unsigned * binshard_p)
{
    unsigned binshard;
    if (tsdn == nullptr || tsdn->arena == nullptr)
        binshard = 0;
    else
        binshard = tsdn->binshards.binshard[binind];
    JE_ASSERT(binshard < bin_infos[binind].n_shards);
    if (binshard_p != nullptr)
        *binshard_p = binshard;
    return arenaGetBin(arena, binind, binshard);
}

}
