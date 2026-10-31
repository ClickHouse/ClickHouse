/* The reference: jemalloc's `arena.c`, `bin.c`, `large.c` (linked from lib_jemalloc.a), called on a manual arena created
 * with `arena_new` (not `arena_init`, to avoid creating a background thread for it), with the real tsd of the reference
 * library. The library is initialized by its constructor (ClickHouse's configuration); its background threads are
 * disabled (the state flag) so that the decay decisions do not depend on them. */

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

/* The reference library is compiled with these definitions (contrib/jemalloc-cmake/CMakeLists.txt); they change the
 * layout of the internal structures (e.g. the tsd). */
#define JEMALLOC_PROF 1
#define JEMALLOC_PROF_LIBUNWIND 1

#include "jemalloc/internal/jemalloc_internal_includes.h"
#include "jemalloc/internal/jemalloc_preamble.h"

#include "jemalloc/internal/arena_inlines_b.h"
#include "jemalloc/internal/emap.h"
#include "jemalloc/internal/extent.h"
#include "jemalloc/internal/sc.h"
#include "jemalloc/internal/sz.h"

#include <stdlib.h>
#include <string.h>

#include "arena_oracle_ref.h"

static tsd_t * ref_thread_state(void)
{
    return tsd_fetch();
}

void ref_boot(RefGlobals * g)
{
    /* Initialize the library and the tsd of this thread (the tsd is not bound to an arena: bin shard 0). */
    void * p = je_malloc(1);
    je_free(p);
    (void)ref_thread_state();
    background_thread_enabled_set_impl(false);

    g->num_arenas_auto = narenas_auto;
    g->manual_arena_base = manual_arena_base;
    g->num_arenas_total = narenas_total_get();
    g->oversize_threshold = oversize_threshold;
    g->dirty_decay_ms_default = arena_dirty_decay_ms_default_get();
    g->muzzy_decay_ms_default = arena_muzzy_decay_ms_default_get();
    g->option_profiling = opt_prof;
    g->option_retain = opt_retain;
    g->option_cache_oblivious = opt_cache_oblivious;
    g->large_pad = sz_large_pad;
    g->calloc_madvise_threshold = opt_calloc_madvise_threshold;
    g->log2_extent_max_active_fit = opt_lg_extent_max_active_fit;
    g->sizeof_arena = sizeof(arena_t);
    g->sizeof_bin = sizeof(bin_t);
    unsigned n = 0;
    for (unsigned i = 0; i < SC_NBINS; i++)
        n += bin_infos[i].n_shards;
    g->num_bins_total = n;
}

void ref_thread_state_rng_get(uint64_t * prng_state, int32_t * tick, int32_t * num_ticks)
{
    tsd_t * thread_state = ref_thread_state();
    *prng_state = *tsd_prng_statep_get(thread_state);
    ticker_geom_t * t = tsd_arena_decay_tickerp_get(thread_state);
    *tick = t->tick;
    *num_ticks = t->nticks;
}

void ref_thread_state_rng_set(uint64_t prng_state, int32_t tick, int32_t num_ticks)
{
    tsd_t * thread_state = ref_thread_state();
    *tsd_prng_statep_get(thread_state) = prng_state;
    ticker_geom_t * t = tsd_arena_decay_tickerp_get(thread_state);
    t->tick = tick;
    t->nticks = num_ticks;
}

void * ref_arena_new(unsigned idx)
{
    return arena_new(tsd_tsdn(ref_thread_state()), idx, &arena_config_default);
}

unsigned ref_arena_idx(void * arena)
{
    return arena_ind_get((arena_t *)arena);
}

void ref_arena_name(void * arena, char * name)
{
    arena_name_get((arena_t *)arena, name);
}

void * ref_malloc_hard(void * arena, size_t size, unsigned idx, bool zero, bool slab)
{
    return arena_malloc_hard(tsd_tsdn(ref_thread_state()), (arena_t *)arena, size, idx, zero, slab);
}

void * ref_allocate_aligned(void * arena, size_t usable_size, size_t alignment, bool zero, bool slab)
{
    return arena_palloc(tsd_tsdn(ref_thread_state()), (arena_t *)arena, usable_size, alignment, zero, slab, NULL);
}

void ref_deallocate_no_thread_cache(void * ptr)
{
    arena_dalloc_no_tcache(tsd_tsdn(ref_thread_state()), ptr);
}

void ref_sized_deallocate_no_thread_cache(void * ptr, size_t size)
{
    arena_sdalloc_no_tcache(tsd_tsdn(ref_thread_state()), ptr, size);
}

unsigned ref_fill_small(void * arena, unsigned bin_idx, void ** ptrs, unsigned num_fill_min, unsigned num_fill_max, uint64_t num_requests)
{
    cache_bin_ptr_array_t array;
    array.n = (cache_bin_sz_t)num_fill_max;
    array.ptr = ptrs;
    cache_bin_stats_t stats;
    stats.nrequests = num_requests;
    return arena_ptr_array_fill_small(
        tsd_tsdn(ref_thread_state()), (arena_t *)arena, bin_idx, &array, (cache_bin_sz_t)num_fill_min, (cache_bin_sz_t)num_fill_max, stats);
}

size_t ref_fill_small_fresh(void * arena, unsigned bin_idx, void ** ptrs, size_t num_fill, bool zero)
{
    return arena_fill_small_fresh(tsd_tsdn(ref_thread_state()), (arena_t *)arena, bin_idx, ptrs, num_fill, zero);
}

void ref_flush(unsigned bin_idx, void ** ptrs, unsigned num_flush, bool small, void * stats_arena, uint64_t num_requests)
{
    cache_bin_ptr_array_t array;
    array.n = (cache_bin_sz_t)num_flush;
    array.ptr = ptrs;
    cache_bin_stats_t stats;
    stats.nrequests = num_requests;
    arena_ptr_array_flush(ref_thread_state(), bin_idx, &array, num_flush, small, (arena_t *)stats_arena, stats);
}

bool ref_reallocate_no_move(void * ptr, size_t old_size, size_t size, size_t extra, bool zero, size_t * new_size)
{
    return arena_ralloc_no_move(tsd_tsdn(ref_thread_state()), ptr, old_size, size, extra, zero, new_size);
}

void * ref_reallocate(void * arena, void * ptr, size_t old_size, size_t size, size_t alignment, bool zero, bool slab)
{
    hook_ralloc_args_t hook_args = {true, {(uintptr_t)ptr, (uintptr_t)size, 0, 0}};
    return arena_ralloc(tsd_tsdn(ref_thread_state()), (arena_t *)arena, ptr, old_size, size, alignment, zero, slab, NULL, &hook_args);
}

size_t ref_allocation_size(const void * ptr)
{
    return arena_salloc(tsd_tsdn(ref_thread_state()), ptr);
}

size_t ref_allocation_size_if_owned(const void * ptr)
{
    return arena_vsalloc(tsd_tsdn(ref_thread_state()), ptr);
}

void ref_decay(void * arena, bool all)
{
    arena_decay(tsd_tsdn(ref_thread_state()), (arena_t *)arena, false, all);
}

bool ref_decay_ms_set(void * arena, int which, ssize_t ms)
{
    return arena_decay_ms_set(tsd_tsdn(ref_thread_state()), (arena_t *)arena, which == 0 ? extent_state_dirty : extent_state_muzzy, ms);
}

void ref_reset(void * arena)
{
    arena_reset(ref_thread_state(), (arena_t *)arena);
}

void ref_destroy(void * arena)
{
    arena_destroy(ref_thread_state(), (arena_t *)arena);
}

void ref_profiling_promote(void * ptr, size_t usable_size, size_t bumped_usable_size)
{
    arena_prof_promote(tsd_tsdn(ref_thread_state()), ptr, usable_size, bumped_usable_size);
}

void ref_profiling_thread_context_reset(void * ptr)
{
    arena_prof_tctx_reset(ref_thread_state(), ptr, NULL);
}

void ref_deallocate_promoted(void * ptr)
{
    arena_dalloc_promoted(tsd_tsdn(ref_thread_state()), ptr, NULL, true);
}

void ref_bin_state(
    void * arena,
    unsigned bin_idx,
    uintptr_t * current_slab,
    unsigned * current_slab_num_free,
    uintptr_t * non_full_first,
    size_t * num_full)
{
    bin_t * bin = arena_get_bin((arena_t *)arena, bin_idx, 0);
    *current_slab = bin->slabcur ? (uintptr_t)edata_addr_get(bin->slabcur) : 0;
    *current_slab_num_free = bin->slabcur ? edata_nfree_get(bin->slabcur) : 0;
    edata_t * first = edata_heap_first(&bin->slabs_nonfull);
    *non_full_first = first ? (uintptr_t)edata_addr_get(first) : 0;
    size_t n = 0;
    for (edata_t * e = edata_list_active_first(&bin->slabs_full); e != NULL; e = edata_list_active_next(&bin->slabs_full, e))
        n++;
    *num_full = n;
}

size_t ref_large_list(void * arena, uintptr_t * out, size_t max)
{
    arena_t * a = (arena_t *)arena;
    size_t n = 0;
    for (edata_t * e = edata_list_active_first(&a->large); e != NULL; e = edata_list_active_next(&a->large, e))
    {
        if (n < max)
            out[n] = (uintptr_t)edata_addr_get(e);
        n++;
    }
    return n;
}

static void put(uint64_t * out, size_t max, size_t * n, uint64_t v)
{
    if (*n < max)
        out[*n] = v;
    (*n)++;
}

static void put_mutex(uint64_t * out, size_t max, size_t * n, const mutex_prof_data_t * d)
{
    put(out, max, n, d->n_lock_ops);
    put(out, max, n, d->n_owner_switches);
    put(out, max, n, d->n_wait_times);
    put(out, max, n, d->n_spin_acquired);
    put(out, max, n, d->max_n_thds);
}

size_t ref_stats(void * arena, uint64_t * out, size_t max)
{
    static arena_stats_t astats;
    static bin_stats_data_t bstats[SC_NBINS];
    static arena_stats_large_t lstats[SC_NSIZES - SC_NBINS];
    static pac_estats_t estats[SC_NPSIZES];
    static hpa_shard_stats_t hpastats;
    memset(&astats, 0, sizeof(astats));
    memset(bstats, 0, sizeof(bstats));
    memset(lstats, 0, sizeof(lstats));
    memset(estats, 0, sizeof(estats));
    memset(&hpastats, 0, sizeof(hpastats));

    unsigned num_threads = 0;
    const char * sbrk = NULL;
    ssize_t dirty_decay_ms = 0;
    ssize_t muzzy_decay_ms = 0;
    size_t num_active = 0;
    size_t num_dirty = 0;
    size_t num_muzzy = 0;
    arena_stats_merge(
        tsd_tsdn(ref_thread_state()),
        (arena_t *)arena,
        &num_threads,
        &sbrk,
        &dirty_decay_ms,
        &muzzy_decay_ms,
        &num_active,
        &num_dirty,
        &num_muzzy,
        &astats,
        bstats,
        lstats,
        estats,
        &hpastats);

    size_t n = 0;
    put(out, max, &n, num_threads);
    uint64_t sbrk_idx = 99;
    for (unsigned i = 0; i < dss_prec_limit; i++)
        if (strcmp(sbrk, dss_prec_names[i]) == 0)
            sbrk_idx = i;
    put(out, max, &n, sbrk_idx);
    put(out, max, &n, (uint64_t)dirty_decay_ms);
    put(out, max, &n, (uint64_t)muzzy_decay_ms);
    put(out, max, &n, num_active);
    put(out, max, &n, num_dirty);
    put(out, max, &n, num_muzzy);

    put(out, max, &n, astats.base);
    put(out, max, &n, astats.metadata_edata);
    put(out, max, &n, astats.metadata_rtree);
    put(out, max, &n, astats.resident);
    put(out, max, &n, astats.metadata_thp);
    put(out, max, &n, astats.mapped);
    put(out, max, &n, atomic_load_zu(&astats.internal, ATOMIC_RELAXED));
    put(out, max, &n, astats.allocated_large);
    put(out, max, &n, astats.nmalloc_large);
    put(out, max, &n, astats.ndalloc_large);
    put(out, max, &n, astats.nfills_large);
    put(out, max, &n, astats.nflushes_large);
    put(out, max, &n, astats.nrequests_large);
    put(out, max, &n, astats.pa_shard_stats.edata_avail);
    const pac_stats_t * page_allocator = &astats.pa_shard_stats.pac_stats;
    put(out, max, &n, locked_read_u64_unsynchronized(&page_allocator->decay_dirty.npurge));
    put(out, max, &n, locked_read_u64_unsynchronized(&page_allocator->decay_dirty.nmadvise));
    put(out, max, &n, locked_read_u64_unsynchronized(&page_allocator->decay_dirty.purged));
    put(out, max, &n, locked_read_u64_unsynchronized(&page_allocator->decay_muzzy.npurge));
    put(out, max, &n, locked_read_u64_unsynchronized(&page_allocator->decay_muzzy.nmadvise));
    put(out, max, &n, locked_read_u64_unsynchronized(&page_allocator->decay_muzzy.purged));
    put(out, max, &n, page_allocator->retained);
    put(out, max, &n, atomic_load_zu(&page_allocator->pac_mapped, ATOMIC_RELAXED));
    put(out, max, &n, atomic_load_zu(&page_allocator->abandoned_vm, ATOMIC_RELAXED));
    put(out, max, &n, astats.tcache_bytes);
    put(out, max, &n, astats.tcache_stashed_bytes);
    for (unsigned i = 0; i < mutex_prof_num_arena_mutexes; i++)
        put_mutex(out, max, &n, &astats.mutex_prof_data[i]);

    for (unsigned i = 0; i < SC_NBINS; i++)
    {
        const bin_stats_t * b = &bstats[i].stats_data;
        put(out, max, &n, b->nmalloc);
        put(out, max, &n, b->ndalloc);
        put(out, max, &n, b->nrequests);
        put(out, max, &n, b->curregs);
        put(out, max, &n, b->nfills);
        put(out, max, &n, b->nflushes);
        put(out, max, &n, b->nslabs);
        put(out, max, &n, b->reslabs);
        put(out, max, &n, b->curslabs);
        put(out, max, &n, b->nonfull_slabs);
        put_mutex(out, max, &n, &bstats[i].mutex_data);
    }
    for (unsigned i = 0; i < SC_NSIZES - SC_NBINS; i++)
    {
        const arena_stats_large_t * l = &lstats[i];
        put(out, max, &n, locked_read_u64_unsynchronized(&l->nmalloc));
        put(out, max, &n, locked_read_u64_unsynchronized(&l->ndalloc));
        put(out, max, &n, locked_read_u64_unsynchronized(&l->active_bytes));
        put(out, max, &n, locked_read_u64_unsynchronized(&l->nrequests));
        put(out, max, &n, locked_read_u64_unsynchronized(&l->nfills));
        put(out, max, &n, locked_read_u64_unsynchronized(&l->nflushes));
        put(out, max, &n, l->curlextents);
    }
    for (unsigned i = 0; i < SC_NPSIZES; i++)
    {
        const pac_estats_t * e = &estats[i];
        put(out, max, &n, e->ndirty);
        put(out, max, &n, e->dirty_bytes);
        put(out, max, &n, e->nmuzzy);
        put(out, max, &n, e->muzzy_bytes);
        put(out, max, &n, e->nretained);
        put(out, max, &n, e->retained_bytes);
    }
    return n;
}

/* --- Interposition of mmap / munmap (address normalization) and clock_gettime (a fake clock) ------------------------
 *
 * The same technique as page_allocator_oracle_ref.c: every mapping created while a side is set is recorded as a region
 * owned by that side; munmap trims or splits regions. An address is normalized to (rank of its region among the side's
 * regions that contain normalized addresses, in creation order; offset in the region). */

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
            r->start = r->end = 0;
        else if (a <= r->start)
            r->start = b;
        else if (b >= r->end)
            r->end = a;
        else
        {
            uintptr_t old_end = r->end;
            r->end = a;
            ref_region_add(b, old_end, r->seq, r->side, r->has_extents);
        }
    }
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
