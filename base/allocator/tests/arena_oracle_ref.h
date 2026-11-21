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
    unsigned num_arenas_auto;
    unsigned manual_arena_base;
    unsigned num_arenas_total;
    size_t oversize_threshold;
    ssize_t dirty_decay_ms_default;
    ssize_t muzzy_decay_ms_default;
    bool option_profiling;
    bool option_retain;
    bool option_cache_oblivious;
    size_t large_pad;
    size_t calloc_madvise_threshold;
    size_t log2_extent_max_active_fit;
    size_t sizeof_arena;
    size_t sizeof_bin;
    unsigned num_bins_total;
} RefGlobals;

/* Initializes the reference tsd (outside of any traced side), disables the reference background threads. */
void ref_boot(RefGlobals * globals);

void ref_trace_set_side(int side);
bool ref_normalize(int side, uintptr_t addr, size_t * rank, size_t * offset);
void ref_clock_set(uint64_t ns);
uint64_t ref_clock_get(void);

/* The tsd state that drives the arena's randomized decisions (the shared PRNG and the decay ticker). */
void ref_thread_state_rng_get(uint64_t * prng_state, int32_t * tick, int32_t * num_ticks);
void ref_thread_state_rng_set(uint64_t prng_state, int32_t tick, int32_t num_ticks);

void * ref_arena_new(unsigned idx);
unsigned ref_arena_idx(void * arena);
void ref_arena_name(void * arena, char * name);

void * ref_malloc_hard(void * arena, size_t size, unsigned idx, bool zero, bool slab);
void * ref_allocate_aligned(void * arena, size_t usable_size, size_t alignment, bool zero, bool slab);
void ref_deallocate_no_thread_cache(void * ptr);
void ref_sized_deallocate_no_thread_cache(void * ptr, size_t size);
unsigned ref_fill_small(void * arena, unsigned bin_idx, void ** ptrs, unsigned num_fill_min, unsigned num_fill_max, uint64_t num_requests);
size_t ref_fill_small_fresh(void * arena, unsigned bin_idx, void ** ptrs, size_t num_fill, bool zero);
void ref_flush(unsigned bin_idx, void ** ptrs, unsigned num_flush, bool small, void * stats_arena, uint64_t num_requests);
bool ref_reallocate_no_move(void * ptr, size_t old_size, size_t size, size_t extra, bool zero, size_t * new_size);
void * ref_reallocate(void * arena, void * ptr, size_t old_size, size_t size, size_t alignment, bool zero, bool slab);
size_t ref_allocation_size(const void * ptr);
size_t ref_allocation_size_if_owned(const void * ptr);
void ref_decay(void * arena, bool all);
bool ref_decay_ms_set(void * arena, int which, ssize_t ms);
void ref_reset(void * arena);
void ref_destroy(void * arena);
void ref_profiling_promote(void * ptr, size_t usable_size, size_t bumped_usable_size);
void ref_deallocate_promoted(void * ptr);
/* What `prof_malloc` does for an allocation that is not sampled (`opt_prof`): reset the tctx of a large extent. */
void ref_profiling_thread_context_reset(void * ptr);

/* The bin state: the slabcur (address, nfree), nonfull heap size, full list (manual arenas). */
void ref_bin_state(
    void * arena,
    unsigned bin_idx,
    uintptr_t * current_slab,
    unsigned * current_slab_num_free,
    uintptr_t * non_full_first,
    size_t * num_full);
/* The extents in the large list of the arena (in list order). */
size_t ref_large_list(void * arena, uintptr_t * out, size_t max);

/* Flattened `arena_stats_merge` output; see `ourStats` in arena_oracle.cpp for the order. */
size_t ref_stats(void * arena, uint64_t * out, size_t max);

#ifdef __cplusplus
}
#endif
