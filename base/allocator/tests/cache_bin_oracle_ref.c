/* The reference: jemalloc's `cache_bin.h` (static inline functions) and `cache_bin.c` (from `lib_jemalloc.a`). */

/* The reference library is built with these (`contrib/jemalloc-cmake/CMakeLists.txt`). */
#define _GNU_SOURCE
#define JEMALLOC_PROF 1

#include "jemalloc/internal/jemalloc_preamble.h"
#include "jemalloc/internal/jemalloc_internal_includes.h"

size_t ref_cache_bin_size(void)
{
    return sizeof(cache_bin_t);
}

size_t ref_cache_bin_ncached_max_limit(void)
{
    return CACHE_BIN_NCACHED_MAX;
}

size_t ref_cache_bin_nflush_batch_max(void)
{
    return CACHE_BIN_NFLUSH_BATCH_MAX;
}

size_t ref_tcache_sizes(size_t * slow_size)
{
    *slow_size = sizeof(tcache_slow_t);
    return sizeof(tcache_t);
}

size_t ref_te_data_size(void)
{
    return sizeof(te_data_t);
}

/* Lays out `nbins` bins in `mem` (as `tcache_init` does); returns the used size. */
size_t ref_cache_bins_init(cache_bin_t * bins, const uint16_t * ncached_max, unsigned nbins, void * mem, size_t * computed_size,
    size_t * computed_alignment)
{
    cache_bin_info_t infos[64];
    for (unsigned i = 0; i < nbins; i++)
        cache_bin_info_init(&infos[i], ncached_max[i]);
    cache_bin_info_compute_alloc(infos, nbins, computed_size, computed_alignment);
    size_t cur_offset = 0;
    cache_bin_preincrement(infos, nbins, mem, &cur_offset);
    for (unsigned i = 0; i < nbins; i++)
        cache_bin_init(&bins[i], &infos[i], mem, &cur_offset);
    cache_bin_postincrement(mem, &cur_offset);
    return cur_offset;
}

void ref_cache_bin_init_disabled(cache_bin_t * bin, uint16_t ncached_max)
{
    cache_bin_init_disabled(bin, ncached_max);
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

bool ref_cache_bin_dalloc_easy(cache_bin_t * bin, void * ptr)
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

uint16_t ref_cache_bin_ncached_get_local(cache_bin_t * bin)
{
    return cache_bin_ncached_get_local(bin);
}

uint16_t ref_cache_bin_nstashed_get_local(cache_bin_t * bin)
{
    return cache_bin_nstashed_get_local(bin);
}

void ref_cache_bin_nitems_get_remote(cache_bin_t * bin, uint16_t * ncached, uint16_t * nstashed)
{
    cache_bin_nitems_get_remote(bin, ncached, nstashed);
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
void ** ref_cache_bin_fill_begin(cache_bin_t * bin, uint16_t nfill)
{
    CACHE_BIN_PTR_ARRAY_DECLARE(arr, nfill);
    cache_bin_init_ptr_array_for_fill(bin, &arr, nfill);
    return arr.ptr;
}

void ref_cache_bin_fill_finish(cache_bin_t * bin, uint16_t nfill, void ** ptr, uint16_t nfilled)
{
    cache_bin_ptr_array_t arr;
    arr.n = nfill;
    arr.ptr = ptr;
    cache_bin_finish_fill(bin, &arr, nfilled);
}

void ** ref_cache_bin_flush_begin(cache_bin_t * bin, uint16_t nflush)
{
    CACHE_BIN_PTR_ARRAY_DECLARE(arr, nflush);
    cache_bin_init_ptr_array_for_flush(bin, &arr, nflush);
    return arr.ptr;
}

void ref_cache_bin_flush_finish(cache_bin_t * bin, uint16_t nflush, void ** ptr, uint16_t nflushed)
{
    cache_bin_ptr_array_t arr;
    arr.n = nflush;
    arr.ptr = ptr;
    cache_bin_finish_flush(bin, &arr, nflushed);
}

void ** ref_cache_bin_flush_stashed_begin(cache_bin_t * bin, uint16_t nstashed)
{
    CACHE_BIN_PTR_ARRAY_DECLARE(arr, nstashed);
    cache_bin_init_ptr_array_for_stashed(bin, 0, &arr, nstashed);
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
