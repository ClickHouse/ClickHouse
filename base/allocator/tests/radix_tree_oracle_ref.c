/* The reference: jemalloc's radix tree (`rtree.h`, `rtree_tsd.h`, `rtree.c`) and the `edata_t` layout. */

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include "jemalloc/internal/jemalloc_internal_includes.h"
#include "jemalloc/internal/jemalloc_preamble.h"

#include "jemalloc/internal/base.h"
#include "jemalloc/internal/edata.h"
#include "jemalloc/internal/pages.h"
#include "jemalloc/internal/rtree.h"
#include "jemalloc/internal/sc.h"
#include "jemalloc/internal/sz.h"

#include <stddef.h>
#include <stdlib.h>

static sc_data_t ref_size_class_data;
static rtree_t * ref_tree;
static rtree_ctx_t ref_context;

/* Geometry and layout constants, by index. */
size_t ref_constant(int which)
{
    switch (which)
    {
        case 0: return RTREE_NHIB;
        case 1: return RTREE_NLIB;
        case 2: return RTREE_NSB;
        case 3: return RTREE_HEIGHT;
#ifdef RTREE_LEAF_COMPACT
        case 4: return 1;
#else
        case 4: return 0;
#endif
        case 5: return rtree_leaf_maskbits();
        case 6: return sizeof(rtree_ctx_t);
        case 7: return sizeof(rtree_t);
        case 8: return sizeof(rtree_leaf_elm_t);
        case 9: return sizeof(rtree_node_elm_t);
        case 10: return RTREE_CTX_NCACHE;
        case 11: return RTREE_CTX_NCACHE_L2;
        case 12: return offsetof(rtree_t, root);
        case 13: return sizeof(((rtree_t *)0)->root) / sizeof(((rtree_t *)0)->root[0]);
        case 14: return sizeof(emap_t);
        /* edata_t */
        case 20: return sizeof(edata_t);
        case 21: return offsetof(edata_t, e_addr);
        case 22: return offsetof(edata_t, e_size_esn);
        case 23: return offsetof(edata_t, e_ps);
        case 24: return offsetof(edata_t, e_sn);
        case 25: return offsetof(edata_t, ql_link_active);
        case 26: return offsetof(edata_t, heap_link);
        case 27: return offsetof(edata_t, ql_link_inactive);
        case 28: return offsetof(edata_t, e_slab_data);
        case 29: return offsetof(edata_t, e_prof_info);
        case 30: return sizeof(slab_data_t);
        case 31: return sizeof(e_prof_info_t);
        case 32: return offsetof(e_prof_info_t, e_prof_frag_link);
        case 33: return offsetof(e_prof_info_t, e_prof_frag_tracked);
        case 34: return EDATA_ALIGNMENT;
        case 35: return ESET_ENUMERATE_MAX_NUM;
        case 40: return EDATA_BITS_ARENA_SHIFT;
        case 41: return EDATA_BITS_SLAB_SHIFT;
        case 42: return EDATA_BITS_COMMITTED_SHIFT;
        case 43: return EDATA_BITS_PAI_SHIFT;
        case 44: return EDATA_BITS_ZEROED_SHIFT;
        case 45: return EDATA_BITS_GUARDED_SHIFT;
        case 46: return EDATA_BITS_STATE_SHIFT;
        case 47: return EDATA_BITS_SZIND_SHIFT;
        case 48: return EDATA_BITS_NFREE_SHIFT;
        case 49: return EDATA_BITS_BINSHARD_SHIFT;
        case 50: return EDATA_BITS_IS_HEAD_SHIFT;
        case 51: return EDATA_BITS_SZIND_WIDTH;
        case 52: return EDATA_BITS_NFREE_WIDTH;
        default: return (size_t)-1;
    }
}

void ref_level(unsigned level, unsigned * bits, unsigned * cumulative_bits)
{
    *bits = rtree_levels[level].bits;
    *cumulative_bits = rtree_levels[level].cumbits;
}

uintptr_t ref_leaf_key(uintptr_t key)
{
    return rtree_leafkey(key);
}

uintptr_t ref_subkey(uintptr_t key, unsigned level)
{
    return rtree_subkey(key, level);
}

size_t ref_direct_map(uintptr_t key)
{
    return rtree_cache_direct_map(key);
}

/* The contents are passed as {edata, szind, state, is_head, slab}. */
static rtree_contents_t make_contents(uintptr_t extent, unsigned size_class_idx, unsigned state, int is_head, int slab)
{
    rtree_contents_t contents;
    contents.edata = (edata_t *)extent;
    contents.metadata.szind = size_class_idx;
    contents.metadata.state = (extent_state_t)state;
    contents.metadata.is_head = is_head;
    contents.metadata.slab = slab;
    return contents;
}

static void split_contents(rtree_contents_t contents, uintptr_t * out)
{
    out[0] = (uintptr_t)contents.edata;
    out[1] = contents.metadata.szind;
    out[2] = contents.metadata.state;
    out[3] = contents.metadata.is_head;
    out[4] = contents.metadata.slab;
}

/* The encoded element: out[0] = bits / edata pointer, out[1] = additional (non-compact). */
void ref_encode(uintptr_t extent, unsigned size_class_idx, unsigned state, int is_head, int slab, uintptr_t * out)
{
    void * bits;
    unsigned additional = 0;
    rtree_contents_encode(make_contents(extent, size_class_idx, state, is_head, slab), &bits, &additional);
    out[0] = (uintptr_t)bits;
#ifdef RTREE_LEAF_COMPACT
    out[1] = 0;
#else
    out[1] = additional;
#endif
}

void ref_decode(uintptr_t bits, uintptr_t * out)
{
#ifdef RTREE_LEAF_COMPACT
    split_contents(rtree_leaf_elm_bits_decode(bits), out);
#else
    (void)bits;
    out[0] = 0;
#endif
}

/* --- A functional tree on a private base --------------------------------------------------------------------- */

int ref_init(void)
{
    sc_boot(&ref_size_class_data);
    sz_boot(&ref_size_class_data, opt_cache_oblivious);
    if (pages_boot())
        return 1;
    base_t * base = base_new(TSDN_NULL, 0, &ehooks_default_extent_hooks, true);
    if (base == NULL)
        return 1;
    ref_tree = calloc(1, sizeof(rtree_t));
    if (ref_tree == NULL || rtree_new(ref_tree, base, true))
        return 1;
    rtree_ctx_data_init(&ref_context);
    return 0;
}

/* The cache: leafkeys and leaf pointers of L1 then L2. */
void ref_context_get(uintptr_t * leaf_keys, uintptr_t * leaves)
{
    for (unsigned i = 0; i < RTREE_CTX_NCACHE; i++)
    {
        leaf_keys[i] = ref_context.cache[i].leafkey;
        leaves[i] = (uintptr_t)ref_context.cache[i].leaf;
    }
    for (unsigned i = 0; i < RTREE_CTX_NCACHE_L2; i++)
    {
        leaf_keys[RTREE_CTX_NCACHE + i] = ref_context.l2_cache[i].leafkey;
        leaves[RTREE_CTX_NCACHE + i] = (uintptr_t)ref_context.l2_cache[i].leaf;
    }
}

uintptr_t ref_lookup(uintptr_t key, int dependent, int init_missing)
{
    return (uintptr_t)rtree_leaf_elm_lookup(TSDN_NULL, ref_tree, &ref_context, key, dependent, init_missing);
}

int ref_write(uintptr_t key, uintptr_t extent, unsigned size_class_idx, unsigned state, int is_head, int slab)
{
    return rtree_write(TSDN_NULL, ref_tree, &ref_context, key, make_contents(extent, size_class_idx, state, is_head, slab));
}

void ref_read(uintptr_t key, uintptr_t * out)
{
    split_contents(rtree_read(TSDN_NULL, ref_tree, &ref_context, key), out);
}

int ref_read_independent(uintptr_t key, uintptr_t * out)
{
    rtree_contents_t contents;
    if (rtree_read_independent(TSDN_NULL, ref_tree, &ref_context, key, &contents))
        return 1;
    split_contents(contents, out);
    return 0;
}

int ref_metadata_try_read_fast(uintptr_t key, uintptr_t * out)
{
    rtree_metadata_t metadata;
    if (rtree_metadata_try_read_fast(TSDN_NULL, ref_tree, &ref_context, key, &metadata))
        return 1;
    out[0] = metadata.szind;
    out[1] = metadata.state;
    out[2] = metadata.is_head;
    out[3] = metadata.slab;
    return 0;
}

void ref_clear(uintptr_t key)
{
    rtree_clear(TSDN_NULL, ref_tree, &ref_context, key);
}

void ref_write_range(uintptr_t base, uintptr_t end, uintptr_t extent, unsigned size_class_idx, unsigned state, int is_head, int slab)
{
    rtree_write_range(TSDN_NULL, ref_tree, &ref_context, base, end, make_contents(extent, size_class_idx, state, is_head, slab));
}

void ref_clear_range(uintptr_t base, uintptr_t end)
{
    rtree_clear_range(TSDN_NULL, ref_tree, &ref_context, base, end);
}

void ref_state_update(uintptr_t key1, uintptr_t key2, unsigned state)
{
    rtree_leaf_elm_t * element1 = rtree_leaf_elm_lookup(TSDN_NULL, ref_tree, &ref_context, key1, true, false);
    rtree_leaf_elm_t * element2 = key2 == 0 ? NULL : rtree_leaf_elm_lookup(TSDN_NULL, ref_tree, &ref_context, key2, true, false);
    rtree_leaf_elm_state_update(TSDN_NULL, ref_tree, element1, element2, (extent_state_t)state);
}
