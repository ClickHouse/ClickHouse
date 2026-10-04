/* The reference: jemalloc's own metadata allocator (`base.c`) and `pages.c`. */

#ifndef _GNU_SOURCE
#    define _GNU_SOURCE
#endif

#include "jemalloc/internal/jemalloc_preamble.h"
#include "jemalloc/internal/jemalloc_internal_includes.h"

#include "jemalloc/internal/base.h"
#include "jemalloc/internal/pages.h"
#include "jemalloc/internal/sc.h"
#include "jemalloc/internal/sz.h"

static sc_data_t ref_sc_data;

/* The relevant part of the boot order of `malloc_init_hard_a0_locked`. */
int ref_boot(void)
{
    sc_boot(&ref_sc_data);
    sz_boot(&ref_sc_data, opt_cache_oblivious);
    return pages_boot();
}

size_t ref_sizeof_base(void)
{
    return sizeof(base_t);
}

size_t ref_sizeof_base_block(void)
{
    return sizeof(base_block_t);
}

void * ref_base_new(unsigned ind)
{
    return base_new(TSDN_NULL, ind, &ehooks_default_extent_hooks, true);
}

void ref_base_delete(void * base)
{
    base_delete(TSDN_NULL, (base_t *)base);
}

void * ref_base_alloc(void * base, size_t size, size_t alignment)
{
    return base_alloc(TSDN_NULL, (base_t *)base, size, alignment);
}

void * ref_base_alloc_edata(void * base, size_t * esn)
{
    edata_t * edata = base_alloc_edata(TSDN_NULL, (base_t *)base);
    if (edata != NULL)
        *esn = edata_esn_get(edata);
    return edata;
}

void * ref_base_alloc_rtree(void * base, size_t size)
{
    return base_alloc_rtree(TSDN_NULL, (base_t *)base, size);
}

void ref_base_stats(void * base, size_t * out)
{
    base_stats_get(TSDN_NULL, (base_t *)base, &out[0], &out[1], &out[2], &out[3], &out[4], &out[5]);
}

/* Blocks, newest first. Returns the count. */
int ref_base_blocks(void * base, uintptr_t * addrs, size_t * sizes, int max)
{
    int n = 0;
    for (base_block_t * b = ((base_t *)base)->blocks; b != NULL && n < max; b = b->next, ++n)
    {
        addrs[n] = (uintptr_t)b;
        sizes[n] = b->size;
    }
    return n;
}

int ref_base_boot(void)
{
    return base_boot(TSDN_NULL);
}

void * ref_b0(void)
{
    return b0get();
}

void * ref_b0_alloc_tcache_stack(size_t size)
{
    return b0_alloc_tcache_stack(TSDN_NULL, size);
}

void ref_b0_dalloc_tcache_stack(void * p)
{
    b0_dalloc_tcache_stack(TSDN_NULL, p);
}
