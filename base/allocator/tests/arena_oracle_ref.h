/* The interface of arena_oracle_ref.c (shared by the C reference and the C++ test). */

#pragma once

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <sys/types.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct
{
    unsigned narenas_auto;
    unsigned manual_arena_base;
    unsigned narenas_total;
    size_t oversize_threshold;
    ssize_t dirty_decay_ms_default;
    ssize_t muzzy_decay_ms_default;
    bool opt_prof;
    bool opt_retain;
    bool opt_cache_oblivious;
    size_t sz_large_pad;
    size_t calloc_madvise_threshold;
    size_t lg_extent_max_active_fit;
    size_t sizeof_arena;
    size_t sizeof_bin;
    unsigned nbins_total;
} ref_globals_t;

/* Initializes the reference tsd (outside of any traced side), disables the reference background threads. */
void ref_boot(ref_globals_t * globals);

void ref_trace_set_side(int side);
bool ref_normalize(int side, uintptr_t addr, size_t * rank, size_t * offset);
void ref_clock_set(uint64_t ns);
uint64_t ref_clock_get(void);

/* The tsd state that drives the arena's randomized decisions (the shared PRNG and the decay ticker). */
void ref_tsd_rng_get(uint64_t * prng_state, int32_t * tick, int32_t * nticks);
void ref_tsd_rng_set(uint64_t prng_state, int32_t tick, int32_t nticks);

void * ref_arena_new(unsigned ind);
unsigned ref_arena_ind(void * arena);
void ref_arena_name(void * arena, char * name);

void * ref_malloc_hard(void * arena, size_t size, unsigned ind, bool zero, bool slab);
void * ref_palloc(void * arena, size_t usize, size_t alignment, bool zero, bool slab);
void ref_dalloc_no_tcache(void * ptr);
void ref_sdalloc_no_tcache(void * ptr, size_t size);
unsigned ref_fill_small(void * arena, unsigned binind, void ** ptrs, unsigned nfill_min, unsigned nfill_max, uint64_t nrequests);
size_t ref_fill_small_fresh(void * arena, unsigned binind, void ** ptrs, size_t nfill, bool zero);
void ref_flush(unsigned binind, void ** ptrs, unsigned nflush, bool small, void * stats_arena, uint64_t nrequests);
bool ref_ralloc_no_move(void * ptr, size_t oldsize, size_t size, size_t extra, bool zero, size_t * newsize);
void * ref_ralloc(void * arena, void * ptr, size_t oldsize, size_t size, size_t alignment, bool zero, bool slab);
size_t ref_salloc(const void * ptr);
size_t ref_vsalloc(const void * ptr);
void ref_decay(void * arena, bool all);
bool ref_decay_ms_set(void * arena, int which, ssize_t ms);
void ref_reset(void * arena);
void ref_destroy(void * arena);
void ref_prof_promote(void * ptr, size_t usize, size_t bumped_usize);
void ref_dalloc_promoted(void * ptr);
/* What `prof_malloc` does for an allocation that is not sampled (`opt_prof`): reset the tctx of a large extent. */
void ref_prof_tctx_reset(void * ptr);

/* The bin state: the slabcur (address, nfree), nonfull heap size, full list (manual arenas). */
void ref_bin_state(void * arena, unsigned binind, uintptr_t * slabcur, unsigned * slabcur_nfree, uintptr_t * nonfull_first, size_t * nfull);
/* The extents in the large list of the arena (in list order). */
size_t ref_large_list(void * arena, uintptr_t * out, size_t max);

/* Flattened `arena_stats_merge` output; see `ourStats` in arena_oracle.cpp for the order. */
size_t ref_stats(void * arena, uint64_t * out, size_t max);

#ifdef __cplusplus
}
#endif
