/* The reference: jemalloc's `eset.c` (linked from lib_jemalloc.a) on fake extents (addresses are never touched). */

#ifndef _GNU_SOURCE
#    define _GNU_SOURCE
#endif

#include "jemalloc/internal/jemalloc_preamble.h"
#include "jemalloc/internal/jemalloc_internal_includes.h"

#include "jemalloc/internal/eset.h"
#include "jemalloc/internal/sc.h"
#include "jemalloc/internal/sz.h"

#include <stdlib.h>
#include <string.h>

static sc_data_t ref_sc_data;

void ref_boot(void)
{
    sc_boot(&ref_sc_data);
    sz_boot(&ref_sc_data, /* cache_oblivious */ true);
}

void ref_set_disable_large_size_classes(bool value)
{
    opt_disable_large_size_classes = value;
}

size_t ref_sizeof_eset(void)
{
    return sizeof(eset_t);
}

void * ref_eset_new(unsigned state)
{
    eset_t * eset = aligned_alloc(64, (sizeof(eset_t) + 63) / 64 * 64);
    memset(eset, 0, sizeof(eset_t));
    eset_init(eset, (extent_state_t)state);
    return eset;
}

void ref_eset_delete(void * eset)
{
    free(eset);
}

void * ref_edata_new(void * addr, size_t size, uint64_t sn, unsigned state)
{
    edata_t * edata = aligned_alloc(EDATA_ALIGNMENT, (sizeof(edata_t) + EDATA_ALIGNMENT - 1) / EDATA_ALIGNMENT * EDATA_ALIGNMENT);
    memset(edata, 0, sizeof(edata_t));
    edata_init(edata, 0, addr, size, false, SC_NSIZES, sn, (extent_state_t)state, false, true, EXTENT_PAI_PAC, EXTENT_NOT_HEAD);
    return edata;
}

void ref_edata_delete(void * edata)
{
    free(edata);
}

void ref_edata_set_state(void * edata, unsigned state)
{
    edata_state_set((edata_t *)edata, (extent_state_t)state);
}

void ref_eset_insert(void * eset, void * edata)
{
    eset_insert((eset_t *)eset, (edata_t *)edata);
}

void ref_eset_remove(void * eset, void * edata)
{
    eset_remove((eset_t *)eset, (edata_t *)edata);
}

void * ref_eset_fit(void * eset, size_t esize, size_t alignment, bool exact_only, unsigned lg_max_fit)
{
    return eset_fit((eset_t *)eset, esize, alignment, exact_only, lg_max_fit);
}

size_t ref_eset_npages(void * eset)
{
    return eset_npages_get((eset_t *)eset);
}

size_t ref_eset_nextents(void * eset, unsigned pind)
{
    return eset_nextents_get((eset_t *)eset, pind);
}

size_t ref_eset_nbytes(void * eset, unsigned pind)
{
    return eset_nbytes_get((eset_t *)eset, pind);
}

unsigned ref_eset_npsizes(void)
{
    return SC_NPSIZES + 1;
}

void ref_eset_heap_min(void * eset, unsigned pind, uint64_t * sn, uintptr_t * addr)
{
    *sn = ((eset_t *)eset)->bins[pind].heap_min.sn;
    *addr = ((eset_t *)eset)->bins[pind].heap_min.addr;
}

bool ref_eset_bin_empty(void * eset, unsigned pind)
{
    return edata_heap_empty(&((eset_t *)eset)->bins[pind].heap);
}

/* The bitmap words. */
size_t ref_eset_bitmap(void * eset, unsigned long * out, size_t max)
{
    size_t n = sizeof(((eset_t *)eset)->bitmap) / sizeof(fb_group_t);
    for (size_t i = 0; i < n && i < max; i++)
        out[i] = ((eset_t *)eset)->bitmap[i];
    return n;
}

/* The LRU list, oldest first. Returns the count. */
size_t ref_eset_lru(void * eset, void ** out, size_t max)
{
    size_t n = 0;
    edata_list_inactive_t * lru = &((eset_t *)eset)->lru;
    for (edata_t * e = edata_list_inactive_first(lru); e != NULL && n < max; e = edata_list_inactive_next(lru, e))
        out[n++] = e;
    return n;
}

/* The pairing heap root and aux count of a bin (to compare the heap shapes). */
void * ref_eset_heap_root(void * eset, unsigned pind, size_t * auxcount)
{
    edata_heap_t * heap = &((eset_t *)eset)->bins[pind].heap;
    *auxcount = heap->ph.auxcount;
    return heap->ph.root;
}
