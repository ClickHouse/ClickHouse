/* The reference: jemalloc's cuckoo hash (`ckh.c`), called with a real tsd of the reference jemalloc. */

#ifndef _GNU_SOURCE
#    define _GNU_SOURCE
#endif

#include "jemalloc/internal/jemalloc_preamble.h"
#include "jemalloc/internal/jemalloc_internal_includes.h"

#include "jemalloc/internal/ckh.h"

static ckh_t ref_table;

/* Initializes the reference jemalloc and returns the tsd of the calling thread. */
static tsd_t * ref_tsd(void)
{
    static int initialized = 0;
    if (!initialized)
    {
        void * p = je_malloc(1);
        je_free(p);
        initialized = 1;
    }
    return tsd_fetch();
}

int ref_ckh_new(size_t minitems, void (*hash)(const void *, size_t[2]), bool (*keycomp)(const void *, const void *))
{
    return ckh_new(ref_tsd(), &ref_table, minitems, hash, keycomp);
}

void ref_ckh_delete(void)
{
    ckh_delete(ref_tsd(), &ref_table);
}

int ref_ckh_insert(const void * key, const void * data)
{
    return ckh_insert(ref_tsd(), &ref_table, key, data);
}

int ref_ckh_remove(const void * searchkey, void ** key, void ** data)
{
    return ckh_remove(ref_tsd(), &ref_table, searchkey, key, data);
}

int ref_ckh_search(const void * searchkey, void ** key, void ** data)
{
    return ckh_search(&ref_table, searchkey, key, data);
}

int ref_ckh_iter(size_t * tabind, void ** key, void ** data)
{
    return ckh_iter(&ref_table, tabind, key, data);
}

size_t ref_ckh_count(void)
{
    return ckh_count(&ref_table);
}

unsigned ref_ckh_lg_minbuckets(void)
{
    return ref_table.lg_minbuckets;
}

unsigned ref_ckh_lg_curbuckets(void)
{
    return ref_table.lg_curbuckets;
}

uint64_t ref_ckh_prng_state(void)
{
    return ref_table.prng_state;
}

const void * ref_ckh_cell_key(size_t i)
{
    return ref_table.tab[i].key;
}

const void * ref_ckh_cell_data(size_t i)
{
    return ref_table.tab[i].data;
}

/* The usable size of the current table (as allocated by `ipallocztm`). */
size_t ref_ckh_table_usize(void)
{
    return je_sallocx(ref_table.tab, 0);
}

size_t ref_sizeof_ckh(void)
{
    return sizeof(ckh_t);
}

unsigned ref_lg_ckh_bucket_cells(void)
{
    return LG_CKH_BUCKET_CELLS;
}
