/* The reference: jemalloc's `cache_bin.h` (static inline functions) and `cache_bin.c` (from `lib_jemalloc.a`). */

/* The reference library is built with these (`contrib/jemalloc-cmake/CMakeLists.txt`). */
#define _GNU_SOURCE
#define JEMALLOC_PROF 1

#include "jemalloc/internal/jemalloc_internal_includes.h"
#include "jemalloc/internal/jemalloc_preamble.h"

size_t ref_cache_bin_size(void)
{
    return sizeof(cache_bin_t);
}

size_t ref_cache_bin_num_cached_max_limit(void)
{
    return CACHE_BIN_NCACHED_MAX;
}

size_t ref_cache_bin_num_flush_batch_max(void)
{
    return CACHE_BIN_NFLUSH_BATCH_MAX;
}

size_t ref_thread_cache_sizes(size_t * slow_size)
{
    *slow_size = sizeof(tcache_slow_t);
    return sizeof(tcache_t);
}

size_t ref_thread_event_data_size(void)
{
    return sizeof(te_data_t);
}

/* Lays out `nbins` bins in `mem` (as `tcache_init` does); returns the used size. */
size_t ref_cache_bins_init(
    cache_bin_t * bins,
    const uint16_t * num_cached_max,
    unsigned num_bins,
    void * memory,
    size_t * computed_size,
    size_t * computed_alignment)
{
    cache_bin_info_t infos[64];
    for (unsigned i = 0; i < num_bins; i++)
        cache_bin_info_init(&infos[i], num_cached_max[i]);
    cache_bin_info_compute_alloc(infos, num_bins, computed_size, computed_alignment);
    size_t current_offset = 0;
    cache_bin_preincrement(infos, num_bins, memory, &current_offset);
    for (unsigned i = 0; i < num_bins; i++)
        cache_bin_init(&bins[i], &infos[i], memory, &current_offset);
    cache_bin_postincrement(memory, &current_offset);
    return current_offset;
}

void ref_cache_bin_init_disabled(cache_bin_t * bin, uint16_t num_cached_max)
{
    cache_bin_init_disabled(bin, num_cached_max);
}

bool ref_cache_bin_disabled(cache_bin_t * bin)
{
    return cache_bin_disabled(bin);
}

const void * ref_disabled_bin(void)
{
    return &disabled_bin;
}

void * ref_cache_bin_alloc_easy(cache_bin_t * bin, bool * success)
{
    return cache_bin_alloc_easy(bin, success);
}

void * ref_cache_bin_alloc(cache_bin_t * bin, bool * success)
{
    return cache_bin_alloc(bin, success);
}

uint16_t ref_cache_bin_alloc_batch(cache_bin_t * bin, size_t num, void ** out)
{
    return cache_bin_alloc_batch(bin, num, out);
}

bool ref_cache_bin_deallocate_easy(cache_bin_t * bin, void * ptr)
{
    return cache_bin_dalloc_easy(bin, ptr);
}

bool ref_cache_bin_stash(cache_bin_t * bin, void * ptr)
{
    return cache_bin_stash(bin, ptr);
}

bool ref_cache_bin_full(cache_bin_t * bin)
{
    return cache_bin_full(bin);
}

void ref_cache_bin_low_water_set(cache_bin_t * bin)
{
    cache_bin_low_water_set(bin);
}

void ref_cache_bin_low_water_adjust(cache_bin_t * bin)
{
    cache_bin_low_water_adjust(bin);
}

uint16_t ref_cache_bin_low_water_get(cache_bin_t * bin)
{
    return cache_bin_low_water_get(bin);
}

uint16_t ref_cache_bin_num_cached_get_local(cache_bin_t * bin)
{
    return cache_bin_ncached_get_local(bin);
}

uint16_t ref_cache_bin_num_stashed_get_local(cache_bin_t * bin)
{
    return cache_bin_nstashed_get_local(bin);
}

void ref_cache_bin_num_items_get_remote(cache_bin_t * bin, uint16_t * num_cached, uint16_t * num_stashed)
{
    cache_bin_nitems_get_remote(bin, num_cached, num_stashed);
}

void ** ref_cache_bin_empty_position_get(cache_bin_t * bin)
{
    return cache_bin_empty_position_get(bin);
}

void ** ref_cache_bin_low_bound_get(cache_bin_t * bin)
{
    return cache_bin_low_bound_get(bin);
}

/* Fill: the arena writes `nfilled` of the `nfill` slots starting at the returned array. */
void ** ref_cache_bin_fill_begin(cache_bin_t * bin, uint16_t num_fill)
{
    CACHE_BIN_PTR_ARRAY_DECLARE(arr, num_fill);
    cache_bin_init_ptr_array_for_fill(bin, &arr, num_fill);
    return arr.ptr;
}

void ref_cache_bin_fill_finish(cache_bin_t * bin, uint16_t num_fill, void ** ptr, uint16_t num_filled)
{
    cache_bin_ptr_array_t array;
    array.n = num_fill;
    array.ptr = ptr;
    cache_bin_finish_fill(bin, &array, num_filled);
}

void ** ref_cache_bin_flush_begin(cache_bin_t * bin, uint16_t num_flush)
{
    CACHE_BIN_PTR_ARRAY_DECLARE(arr, num_flush);
    cache_bin_init_ptr_array_for_flush(bin, &arr, num_flush);
    return arr.ptr;
}

void ref_cache_bin_flush_finish(cache_bin_t * bin, uint16_t num_flush, void ** ptr, uint16_t num_flushed)
{
    cache_bin_ptr_array_t array;
    array.n = num_flush;
    array.ptr = ptr;
    cache_bin_finish_flush(bin, &array, num_flushed);
}

void ** ref_cache_bin_flush_stashed_begin(cache_bin_t * bin, uint16_t num_stashed)
{
    CACHE_BIN_PTR_ARRAY_DECLARE(arr, num_stashed);
    cache_bin_init_ptr_array_for_stashed(bin, 0, &arr, num_stashed);
    return arr.ptr;
}

void ref_cache_bin_flush_stashed_finish(cache_bin_t * bin)
{
    cache_bin_finish_flush_stashed(bin);
}

/* `cache_bin.o` pulls in the rest of the reference jemalloc, including the libunwind-based profiler backtrace, which
 * is never called here. */
__attribute__((weak)) int unw_backtrace(void ** buffer, int size)
{
    (void)buffer;
    (void)size;
    return 0;
}
