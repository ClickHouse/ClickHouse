/* The reference: jemalloc's `eset.c` (linked from lib_jemalloc.a) on fake extents (addresses are never touched). */

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include "jemalloc/internal/jemalloc_internal_includes.h"
#include "jemalloc/internal/jemalloc_preamble.h"

#include "jemalloc/internal/eset.h"
#include "jemalloc/internal/sc.h"
#include "jemalloc/internal/sz.h"

#include <stdlib.h>
#include <string.h>

static sc_data_t ref_size_class_data;

void ref_boot(void)
{
    sc_boot(&ref_size_class_data);
    sz_boot(&ref_size_class_data, /* cache_oblivious */ true);
}

void ref_set_disable_large_size_classes(bool value)
{
    opt_disable_large_size_classes = value;
}

size_t ref_sizeof_extent_set(void)
{
    return sizeof(eset_t);
}

void * ref_extent_set_new(unsigned state)
{
    eset_t * extent_set = aligned_alloc(64, (sizeof(eset_t) + 63) / 64 * 64);
    memset(extent_set, 0, sizeof(eset_t));
    eset_init(extent_set, (extent_state_t)state);
    return extent_set;
}

void ref_extent_set_delete(void * extent_set)
{
    free(extent_set);
}

void * ref_extent_new(void * addr, size_t size, uint64_t serial_number, unsigned state)
{
    edata_t * extent = aligned_alloc(EDATA_ALIGNMENT, (sizeof(edata_t) + EDATA_ALIGNMENT - 1) / EDATA_ALIGNMENT * EDATA_ALIGNMENT);
    memset(extent, 0, sizeof(edata_t));
    edata_init(extent, 0, addr, size, false, SC_NSIZES, serial_number, (extent_state_t)state, false, true, EXTENT_PAI_PAC, EXTENT_NOT_HEAD);
    return extent;
}

void ref_extent_delete(void * extent)
{
    free(extent);
}

void ref_extent_set_state(void * extent, unsigned state)
{
    edata_state_set((edata_t *)extent, (extent_state_t)state);
}

void ref_extent_set_insert(void * extent_set, void * extent)
{
    eset_insert((eset_t *)extent_set, (edata_t *)extent);
}

void ref_extent_set_remove(void * extent_set, void * extent)
{
    eset_remove((eset_t *)extent_set, (edata_t *)extent);
}

void * ref_extent_set_fit(void * extent_set, size_t extent_size, size_t alignment, bool exact_only, unsigned log2_max_fit)
{
    return eset_fit((eset_t *)extent_set, extent_size, alignment, exact_only, log2_max_fit);
}

size_t ref_extent_set_num_pages(void * extent_set)
{
    return eset_npages_get((eset_t *)extent_set);
}

size_t ref_extent_set_num_extents(void * extent_set, unsigned page_size_class_idx)
{
    return eset_nextents_get((eset_t *)extent_set, page_size_class_idx);
}

size_t ref_extent_set_num_bytes(void * extent_set, unsigned page_size_class_idx)
{
    return eset_nbytes_get((eset_t *)extent_set, page_size_class_idx);
}

unsigned ref_extent_set_num_page_sizes(void)
{
    return SC_NPSIZES + 1;
}

void ref_extent_set_heap_min(void * extent_set, unsigned page_size_class_idx, uint64_t * serial_number, uintptr_t * addr)
{
    *serial_number = ((eset_t *)extent_set)->bins[page_size_class_idx].heap_min.sn;
    *addr = ((eset_t *)extent_set)->bins[page_size_class_idx].heap_min.addr;
}

bool ref_extent_set_bin_empty(void * extent_set, unsigned page_size_class_idx)
{
    return edata_heap_empty(&((eset_t *)extent_set)->bins[page_size_class_idx].heap);
}

/* The bitmap words. */
size_t ref_extent_set_bitmap(void * extent_set, unsigned long * out, size_t max)
{
    size_t n = sizeof(((eset_t *)extent_set)->bitmap) / sizeof(fb_group_t);
    for (size_t i = 0; i < n && i < max; i++)
        out[i] = ((eset_t *)extent_set)->bitmap[i];
    return n;
}

/* The LRU list, oldest first. Returns the count. */
size_t ref_extent_set_lru(void * extent_set, void ** out, size_t max)
{
    size_t n = 0;
    edata_list_inactive_t * lru = &((eset_t *)extent_set)->lru;
    for (edata_t * e = edata_list_inactive_first(lru); e != NULL && n < max; e = edata_list_inactive_next(lru, e))
        out[n++] = e;
    return n;
}

/* The pairing heap root and aux count of a bin (to compare the heap shapes). */
void * ref_extent_set_heap_root(void * extent_set, unsigned page_size_class_idx, size_t * auxiliary_count)
{
    edata_heap_t * heap = &((eset_t *)extent_set)->bins[page_size_class_idx].heap;
    *auxiliary_count = heap->ph.auxcount;
    return heap->ph.root;
}
