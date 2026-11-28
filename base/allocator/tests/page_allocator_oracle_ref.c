/* The reference: jemalloc's `pa.c`, `pac.c`, `extent.c`, `ecache.c`, `eset.c` (linked from lib_jemalloc.a) on a
 * private shard with its own base and emap. */

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include "jemalloc/internal/jemalloc_internal_includes.h"
#include "jemalloc/internal/jemalloc_preamble.h"

#include "jemalloc/internal/emap.h"
#include "jemalloc/internal/hpa.h"
#include "jemalloc/internal/pa.h"
#include "jemalloc/internal/pages.h"
#include "jemalloc/internal/sc.h"
#include "jemalloc/internal/sz.h"

#include <stdlib.h>
#include <string.h>

#include "page_allocator_oracle_ref.h"

static sc_data_t ref_size_class_data;

int ref_boot(void)
{
    sc_boot(&ref_size_class_data);
    sz_boot(&ref_size_class_data, opt_cache_oblivious);
    return pages_boot();
}

void ref_set_disable_large_size_classes(bool value)
{
    opt_disable_large_size_classes = value;
}

size_t ref_sizeof_page_allocator_shard(void)
{
    return sizeof(pa_shard_t);
}

size_t ref_sizeof_page_allocator(void)
{
    return sizeof(pac_t);
}

typedef struct
{
    pa_shard_t shard; /* Must be first (64-aligned). */
    pa_central_t central;
    pa_shard_stats_t stats;
    emap_t emap;
    base_t * base;
} ref_shard_t;

void * ref_shard_new(unsigned idx, ssize_t dirty_ms, ssize_t muzzy_ms, size_t oversize_threshold)
{
    size_t size = (sizeof(ref_shard_t) + 63) / 64 * 64;
    ref_shard_t * s = aligned_alloc(64, size);
    memset(s, 0, sizeof(ref_shard_t));
    s->base = base_new(TSDN_NULL, idx, &ehooks_default_extent_hooks, true);
    if (s->base == NULL)
        return NULL;
    if (emap_init(&s->emap, s->base, /* zeroed */ true))
        return NULL;
    nstime_t current;
    nstime_init_update(&current);
    if (pa_shard_init(
            TSDN_NULL, &s->shard, &s->central, &s->emap, s->base, idx, &s->stats, NULL, &current, oversize_threshold, dirty_ms, muzzy_ms))
        return NULL;
    return s;
}

void ref_shard_destroy(void * p)
{
    ref_shard_t * s = p;
    pa_shard_destroy(TSDN_NULL, &s->shard);
}

void * ref_alloc(void * p, size_t size, size_t alignment, bool slab, unsigned size_class_idx, bool zero, bool guarded, bool * deferred)
{
    ref_shard_t * s = p;
    return pa_alloc(TSDN_NULL, &s->shard, size, alignment, slab, size_class_idx, zero, guarded, deferred);
}

bool ref_expand(void * p, void * extent, size_t old_size, size_t new_size, unsigned size_class_idx, bool zero, bool * deferred)
{
    ref_shard_t * s = p;
    return pa_expand(TSDN_NULL, &s->shard, extent, old_size, new_size, size_class_idx, zero, deferred);
}

bool ref_shrink(void * p, void * extent, size_t old_size, size_t new_size, unsigned size_class_idx, bool * deferred)
{
    ref_shard_t * s = p;
    return pa_shrink(TSDN_NULL, &s->shard, extent, old_size, new_size, size_class_idx, deferred);
}

void ref_deallocate(void * p, void * extent, bool * deferred)
{
    ref_shard_t * s = p;
    pa_dalloc(TSDN_NULL, &s->shard, extent, deferred);
}

static void ref_decay_data(ref_shard_t * s, int which, decay_t ** decay, pac_decay_stats_t ** stats, ecache_t ** extent_cache)
{
    if (which == 0)
    {
        *decay = &s->shard.pac.decay_dirty;
        *stats = &s->shard.pac.stats->decay_dirty;
        *extent_cache = &s->shard.pac.ecache_dirty;
    }
    else
    {
        *decay = &s->shard.pac.decay_muzzy;
        *stats = &s->shard.pac.stats->decay_muzzy;
        *extent_cache = &s->shard.pac.ecache_muzzy;
    }
}

bool ref_maybe_decay_purge(void * p, int which, int eagerness)
{
    ref_shard_t * s = p;
    decay_t * decay;
    pac_decay_stats_t * stats;
    ecache_t * extent_cache;
    ref_decay_data(s, which, &decay, &stats, &extent_cache);
    malloc_mutex_lock(TSDN_NULL, &decay->mtx);
    bool r = pac_maybe_decay_purge(TSDN_NULL, &s->shard.pac, decay, stats, extent_cache, (pac_purge_eagerness_t)eagerness);
    malloc_mutex_unlock(TSDN_NULL, &decay->mtx);
    return r;
}

void ref_decay_all(void * p, int which, bool fully_decay)
{
    ref_shard_t * s = p;
    decay_t * decay;
    pac_decay_stats_t * stats;
    ecache_t * extent_cache;
    ref_decay_data(s, which, &decay, &stats, &extent_cache);
    malloc_mutex_lock(TSDN_NULL, &decay->mtx);
    pac_decay_all(TSDN_NULL, &s->shard.pac, decay, stats, extent_cache, fully_decay);
    malloc_mutex_unlock(TSDN_NULL, &decay->mtx);
}

bool ref_decay_ms_set(void * p, int which, ssize_t ms, int eagerness)
{
    ref_shard_t * s = p;
    return pa_decay_ms_set(
        TSDN_NULL, &s->shard, which == 0 ? extent_state_dirty : extent_state_muzzy, ms, (pac_purge_eagerness_t)eagerness);
}

ssize_t ref_decay_ms_get(void * p, int which)
{
    ref_shard_t * s = p;
    return pa_decay_ms_get(&s->shard, which == 0 ? extent_state_dirty : extent_state_muzzy);
}

void ref_decay_state(void * p, int which, uint64_t * epoch_ns, size_t * num_pages_limit, size_t * num_unpurged, size_t * backlog_last)
{
    ref_shard_t * s = p;
    decay_t * decay;
    pac_decay_stats_t * stats;
    ecache_t * extent_cache;
    ref_decay_data(s, which, &decay, &stats, &extent_cache);
    *epoch_ns = nstime_ns(&decay->epoch);
    *num_pages_limit = decay->npages_limit;
    *num_unpurged = decay->nunpurged;
    *backlog_last = decay->backlog[SMOOTHSTEP_NSTEPS - 1];
}

uint64_t ref_time_until_deferred_work(void * p)
{
    ref_shard_t * s = p;
    return pa_shard_time_until_deferred_work(TSDN_NULL, &s->shard);
}

bool ref_retain_grow_limit(void * p, size_t * old_limit, size_t * new_limit)
{
    ref_shard_t * s = p;
    return pac_retain_grow_limit_get_set(TSDN_NULL, &s->shard.pac, old_limit, new_limit);
}

void ref_extent_info(void * p, RefExtentInfo * info)
{
    edata_t * e = p;
    info->addr = (uintptr_t)edata_addr_get(e);
    info->size = edata_size_get(e);
    info->serial_number = edata_sn_get(e);
    info->state = edata_state_get(e);
    info->size_class_idx = edata_szind_get_maybe_invalid(e);
    info->zeroed = edata_zeroed_get(e);
    info->committed = edata_committed_get(e);
    info->guarded = edata_guarded_get(e);
    info->slab = edata_slab_get(e);
    info->is_head = edata_is_head_get(e);
    info->arena_idx = edata_arena_ind_get(e);
}

size_t ref_extent_cache_list(void * p, int which, bool guarded, RefExtentInfo * out, size_t max)
{
    ref_shard_t * s = p;
    ecache_t * extent_cache
        = which == 0 ? &s->shard.pac.ecache_dirty : (which == 1 ? &s->shard.pac.ecache_muzzy : &s->shard.pac.ecache_retained);
    eset_t * extent_set = guarded ? &extent_cache->guarded_eset : &extent_cache->eset;
    size_t n = 0;
    for (edata_t * e = edata_list_inactive_first(&extent_set->lru); e != NULL && n < max; e = edata_list_inactive_next(&extent_set->lru, e))
        ref_extent_info(e, &out[n++]);
    return n;
}

void ref_stats(void * p, uint64_t * out)
{
    ref_shard_t * s = p;
    pa_shard_t * shard = &s->shard;
    size_t num_active = 0;
    size_t num_dirty = 0;
    size_t num_muzzy = 0;
    pa_shard_basic_stats_merge(shard, &num_active, &num_dirty, &num_muzzy);

    static pa_shard_stats_t stats;
    static pac_estats_t estats[SC_NPSIZES];
    static hpa_shard_stats_t huge_page_allocator_stats;
    memset(&stats, 0, sizeof(stats));
    memset(estats, 0, sizeof(estats));
    size_t resident = 0;
    pa_shard_stats_merge(TSDN_NULL, shard, &stats, estats, &huge_page_allocator_stats, &resident);

    size_t i = 0;
    out[i++] = num_active;
    out[i++] = num_dirty;
    out[i++] = num_muzzy;
    out[i++] = resident;
    out[i++] = stats.edata_avail;
    out[i++] = stats.pac_stats.retained;
    out[i++] = atomic_load_zu(&stats.pac_stats.pac_mapped, ATOMIC_RELAXED);
    out[i++] = atomic_load_zu(&stats.pac_stats.abandoned_vm, ATOMIC_RELAXED);
    out[i++] = atomic_load_zu(&shard->pac.stats->pac_mapped, ATOMIC_RELAXED);
    out[i++] = locked_read_u64_unsynchronized(&stats.pac_stats.decay_dirty.npurge);
    out[i++] = locked_read_u64_unsynchronized(&stats.pac_stats.decay_dirty.nmadvise);
    out[i++] = locked_read_u64_unsynchronized(&stats.pac_stats.decay_dirty.purged);
    out[i++] = locked_read_u64_unsynchronized(&stats.pac_stats.decay_muzzy.npurge);
    out[i++] = locked_read_u64_unsynchronized(&stats.pac_stats.decay_muzzy.nmadvise);
    out[i++] = locked_read_u64_unsynchronized(&stats.pac_stats.decay_muzzy.purged);
    out[i++] = atomic_load_zu(&shard->pac.extent_sn_next, ATOMIC_RELAXED);
    out[i++] = shard->pac.exp_grow.next;
    out[i++] = shard->pac.exp_grow.limit;
    out[i++] = atomic_load_zu(&shard->edata_cache.count, ATOMIC_RELAXED);
    for (unsigned j = 0; j < SC_NPSIZES; j++)
    {
        out[i++] = estats[j].ndirty;
        out[i++] = estats[j].dirty_bytes;
        out[i++] = estats[j].nmuzzy;
        out[i++] = estats[j].muzzy_bytes;
        out[i++] = estats[j].nretained;
        out[i++] = estats[j].retained_bytes;
    }
}

size_t ref_num_stats(void)
{
    return 19 + 6 * SC_NPSIZES;
}

/* The emap lookup of an address: the extent and its szind / slab. */
void * ref_extent_map_lookup(void * p, const void * ptr, unsigned * size_class_idx, bool * slab)
{
    ref_shard_t * s = p;
    emap_full_alloc_ctx_t context;
    if (emap_full_alloc_ctx_try_lookup(TSDN_NULL, &s->emap, ptr, &context))
        return NULL;
    *size_class_idx = context.szind;
    *slab = context.slab;
    return context.edata;
}

/* --- Interposition of mmap / munmap (address normalization) and clock_gettime (a fake clock) ------------------------
 *
 * Every mapping created while a side is set is recorded as a region owned by that side; munmap trims or splits
 * regions. An address is normalized to (rank of its region among the side's live regions in creation order, offset
 * in the region), which is identical for both implementations if they map the same sizes in the same order. */

#include <time.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/syscall.h>

typedef struct
{
    uintptr_t start;
    uintptr_t end;
    uint64_t seq;
    int side;
    /* Contains (or contained) an extent; metadata regions of the base are not counted, because the number of rtree
     * leaves depends on the absolute addresses. */
    bool has_extents;
} ref_region_t;

#define REF_MAX_REGIONS 16384
static ref_region_t ref_regions[REF_MAX_REGIONS];
static size_t ref_num_regions;
static uint64_t ref_sequence_next;
static int ref_current_side = -1;
static uint64_t ref_fake_now_ns = (uint64_t)1000 * 1000 * 1000 * 1000;

void ref_trace_set_side(int side)
{
    ref_current_side = side;
}

void ref_clock_set(uint64_t ns)
{
    ref_fake_now_ns = ns;
}

uint64_t ref_clock_get(void)
{
    return ref_fake_now_ns;
}

static void ref_region_add(uintptr_t start, uintptr_t end, uint64_t sequence, int side, bool has_extents)
{
    if (ref_num_regions == REF_MAX_REGIONS)
        abort();
    ref_regions[ref_num_regions].start = start;
    ref_regions[ref_num_regions].end = end;
    ref_regions[ref_num_regions].seq = sequence;
    ref_regions[ref_num_regions].side = side;
    ref_regions[ref_num_regions].has_extents = has_extents;
    ref_num_regions++;
}

static void ref_region_cut(uintptr_t a, uintptr_t b)
{
    size_t n = ref_num_regions;
    for (size_t i = 0; i < n; i++)
    {
        ref_region_t * r = &ref_regions[i];
        if (r->end <= a || r->start >= b || r->start == r->end)
            continue;
        if (a <= r->start && b >= r->end)
        {
            r->start = r->end = 0; /* Removed. */
        }
        else if (a <= r->start)
        {
            r->start = b;
        }
        else if (b >= r->end)
        {
            r->end = a;
        }
        else
        {
            uintptr_t old_end = r->end;
            r->end = a;
            ref_region_add(b, old_end, r->seq, r->side, r->has_extents);
        }
    }
    /* Compact. */
    size_t k = 0;
    for (size_t i = 0; i < ref_num_regions; i++)
        if (ref_regions[i].start != ref_regions[i].end)
            ref_regions[k++] = ref_regions[i];
    ref_num_regions = k;
}

void * mmap(void * addr, size_t len, int protection, int flags, int fd, off_t offset)
{
#if defined(__s390x__)
    /* s390x has only the old `mmap` system call, which takes a pointer to the six arguments. */
    long args[6] = {(long)addr, (long)len, protection, flags, fd, (long)offset};
    void * r = (void *)syscall(SYS_mmap, args);
#else
    void * r = (void *)syscall(SYS_mmap, addr, len, protection, flags, fd, offset);
#endif
    if (r != MAP_FAILED && !(flags & MAP_FIXED) && ref_current_side >= 0)
        ref_region_add((uintptr_t)r, (uintptr_t)r + len, ref_sequence_next++, ref_current_side, false);
    return r;
}

int munmap(void * addr, size_t len)
{
    int r = (int)syscall(SYS_munmap, addr, len);
    if (r == 0)
        ref_region_cut((uintptr_t)addr, (uintptr_t)addr + len);
    return r;
}

int clock_gettime(clockid_t clock_id, struct timespec * time_spec)
{
    if (clock_id == CLOCK_MONOTONIC || clock_id == CLOCK_MONOTONIC_COARSE)
    {
        time_spec->tv_sec = (time_t)(ref_fake_now_ns / 1000000000);
        time_spec->tv_nsec = (long)(ref_fake_now_ns % 1000000000);
        return 0;
    }
    return (int)syscall(SYS_clock_gettime, clock_id, time_spec);
}

/* Returns false if the address is not in a region of the side. */
bool ref_normalize(int side, uintptr_t addr, size_t * rank, size_t * offset)
{
    ref_region_t * found = NULL;
    for (size_t i = 0; i < ref_num_regions; i++)
    {
        ref_region_t * r = &ref_regions[i];
        if (r->side == side && addr >= r->start && addr < r->end)
        {
            found = r;
            break;
        }
    }
    if (found == NULL)
        return false;
    found->has_extents = true;
    size_t k = 0;
    for (size_t i = 0; i < ref_num_regions; i++)
    {
        const ref_region_t * r = &ref_regions[i];
        if (r->side == side && r->has_extents && (r->seq < found->seq || (r->seq == found->seq && r->start < found->start)))
            k++;
    }
    *rank = k;
    *offset = addr - found->start;
    return true;
}

size_t ref_num_regions_of(int side)
{
    size_t k = 0;
    for (size_t i = 0; i < ref_num_regions; i++)
        if (ref_regions[i].side == side && ref_regions[i].has_extents)
            k++;
    return k;
}

/* The reference library is initialized by its constructor with ClickHouse's configuration, so its global
 * `background_thread_enabled_state` is true. */
bool ref_background_thread_enabled(void)
{
    return background_thread_enabled();
}

void ref_set_background_thread_enabled(bool value)
{
    background_thread_enabled_set_impl(value);
}
