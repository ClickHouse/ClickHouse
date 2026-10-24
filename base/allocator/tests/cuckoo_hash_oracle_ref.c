/* The reference: jemalloc's cuckoo hash (`ckh.c`), called with a real tsd of the reference jemalloc. */

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include "jemalloc/internal/jemalloc_internal_includes.h"
#include "jemalloc/internal/jemalloc_preamble.h"

#include "jemalloc/internal/ckh.h"

static ckh_t ref_table;

/* Initializes the reference jemalloc and returns the tsd of the calling thread. */
static tsd_t * ref_thread_state(void)
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

int ref_cuckoo_hash_new(size_t min_items, void (*hash)(const void *, size_t[2]), bool (*key_compare)(const void *, const void *))
{
    return ckh_new(ref_thread_state(), &ref_table, min_items, hash, key_compare);
}

void ref_cuckoo_hash_delete(void)
{
    ckh_delete(ref_thread_state(), &ref_table);
}

int ref_cuckoo_hash_insert(const void * key, const void * data)
{
    return ckh_insert(ref_thread_state(), &ref_table, key, data);
}

int ref_cuckoo_hash_remove(const void * search_key, void ** key, void ** data)
{
    return ckh_remove(ref_thread_state(), &ref_table, search_key, key, data);
}

int ref_cuckoo_hash_search(const void * search_key, void ** key, void ** data)
{
    return ckh_search(&ref_table, search_key, key, data);
}

int ref_cuckoo_hash_iterate(size_t * table_idx, void ** key, void ** data)
{
    return ckh_iter(&ref_table, table_idx, key, data);
}

size_t ref_cuckoo_hash_count(void)
{
    return ckh_count(&ref_table);
}

unsigned ref_cuckoo_hash_log2_min_buckets(void)
{
    return ref_table.lg_minbuckets;
}

unsigned ref_cuckoo_hash_log2_current_buckets(void)
{
    return ref_table.lg_curbuckets;
}

uint64_t ref_cuckoo_hash_prng_state(void)
{
    return ref_table.prng_state;
}

const void * ref_cuckoo_hash_cell_key(size_t i)
{
    return ref_table.tab[i].key;
}

const void * ref_cuckoo_hash_cell_data(size_t i)
{
    return ref_table.tab[i].data;
}

/* The usable size of the current table (as allocated by `ipallocztm`). */
size_t ref_cuckoo_hash_table_usable_size(void)
{
    return je_sallocx(ref_table.tab, 0);
}

size_t ref_sizeof_cuckoo_hash(void)
{
    return sizeof(ckh_t);
}

unsigned ref_log2_cuckoo_hash_bucket_cells(void)
{
    return LG_CKH_BUCKET_CELLS;
}
