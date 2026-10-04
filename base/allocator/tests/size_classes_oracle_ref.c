/* The reference: jemalloc's own size class code (`sc.c`, `sz.c`, `sz.h`, `bin_info.c`, `bin.c`, `div.c`). */

#ifndef _GNU_SOURCE
#    define _GNU_SOURCE
#endif

#include "jemalloc/internal/jemalloc_preamble.h"
#include "jemalloc/internal/jemalloc_internal_includes.h"

#include "jemalloc/internal/bin.h"
#include "jemalloc/internal/bin_info.h"
#include "jemalloc/internal/div.h"
#include "jemalloc/internal/sc.h"
#include "jemalloc/internal/sz.h"

static sc_data_t ref_sc_data;
static unsigned ref_shards[SC_NBINS];

size_t ref_constant(int which)
{
    switch (which)
    {
        case 0: return SC_NSIZES;
        case 1: return SC_NBINS;
        case 2: return SC_NPSIZES;
        case 3: return PAGE;
        case 4: return SC_SMALL_MAXCLASS;
        case 5: return SC_LARGE_MINCLASS;
        case 6: return SC_LARGE_MAXCLASS;
        case 7: return SC_LOOKUP_MAXCLASS;
        case 8: return SC_LG_SLAB_MAXREGS;
        case 9: return LG_BITMAP_MAXBITS;
        case 10: return BITMAP_MAXBITS;
        case 11: return BITMAP_GROUPS_MAX;
        case 12: return sizeof(bitmap_info_t);
        case 13: return sizeof(bin_info_t);
        case 14: return USIZE_GROW_SLOW_THRESHOLD;
        case 15: return SC_NTINY;
        case 16: return (SC_LOOKUP_MAXCLASS >> SC_LG_TINY_MIN) + 1;
        case 17: return BIN_SHARDS_MAX;
        default: return (size_t)-1;
    }
}

/* Mirrors the boot order of `malloc_init_hard_a0_locked`. */
void ref_boot(bool cache_oblivious)
{
    sc_boot(&ref_sc_data);
    bin_shard_sizes_boot(ref_shards);
    sz_boot(&ref_sc_data, cache_oblivious);
    bin_info_boot(&ref_sc_data, ref_shards);
}

void ref_sz_boot(bool cache_oblivious)
{
    sz_boot(&ref_sc_data, cache_oblivious);
}

void ref_sc_data_init(void)
{
    sc_data_init(&ref_sc_data);
}

void ref_sc_update_slab_size(size_t begin, size_t end, int pgs)
{
    sc_data_update_slab_size(&ref_sc_data, begin, end, pgs);
}

void ref_bin_shard_sizes_boot(void)
{
    bin_shard_sizes_boot(ref_shards);
}

bool ref_bin_update_shard_size(size_t start, size_t end, size_t nshards)
{
    return bin_update_shard_size(ref_shards, start, end, nshards);
}

unsigned ref_bin_shard(unsigned i)
{
    return ref_shards[i];
}

void ref_bin_info_boot(void)
{
    bin_info_boot(&ref_sc_data, ref_shards);
}

/* Summary fields of `sc_data_t`. */
void ref_sc_summary(size_t * out)
{
    out[0] = ref_sc_data.ntiny;
    out[1] = (size_t)ref_sc_data.nlbins;
    out[2] = (size_t)ref_sc_data.nbins;
    out[3] = (size_t)ref_sc_data.nsizes;
    out[4] = (size_t)ref_sc_data.lg_ceil_nsizes;
    out[5] = ref_sc_data.npsizes;
    out[6] = (size_t)ref_sc_data.lg_tiny_maxclass;
    out[7] = ref_sc_data.lookup_maxclass;
    out[8] = ref_sc_data.small_maxclass;
    out[9] = (size_t)ref_sc_data.lg_large_minclass;
    out[10] = ref_sc_data.large_minclass;
    out[11] = ref_sc_data.large_maxclass;
    out[12] = ref_sc_data.initialized;
}

void ref_sc(unsigned i, int * out)
{
    const sc_t * sc = &ref_sc_data.sc[i];
    out[0] = sc->index;
    out[1] = sc->lg_base;
    out[2] = sc->lg_delta;
    out[3] = sc->ndelta;
    out[4] = sc->psz;
    out[5] = sc->bin;
    out[6] = sc->pgs;
    out[7] = sc->lg_delta_lookup;
}

static void bitmap_info_fields(const bitmap_info_t * info, size_t * out)
{
    out[0] = info->nbits;
#ifdef BITMAP_USE_TREE
    out[1] = info->nlevels;
    for (unsigned l = 0; l <= BITMAP_MAX_LEVELS; l++)
        out[2 + l] = info->levels[l].group_offset;
#else
    out[1] = info->ngroups;
#endif
}

/* reg_size, slab_size, nregs, n_shards, then the bitmap info fields. */
void ref_bin_info(unsigned i, size_t * out)
{
    const bin_info_t * info = &bin_infos[i];
    out[0] = info->reg_size;
    out[1] = info->slab_size;
    out[2] = info->nregs;
    out[3] = info->n_shards;
    bitmap_info_fields(&info->bitmap_info, out + 4);
}

void ref_set_disable_large_size_classes(bool value)
{
    opt_disable_large_size_classes = value;
}

size_t ref_large_pad(void)
{
    return sz_large_pad;
}

size_t ref_index2size_tab(unsigned i)
{
    return sz_index2size_tab[i];
}

unsigned ref_size2index_tab(unsigned i)
{
    return sz_size2index_tab[i];
}

size_t ref_pind2sz_tab(unsigned i)
{
    return sz_pind2sz_tab[i];
}

unsigned ref_size2index(size_t size)
{
    return sz_size2index(size);
}

unsigned ref_size2index_compute(size_t size)
{
    return sz_size2index_compute(size);
}

unsigned ref_size2index_lookup(size_t size)
{
    return sz_size2index_lookup(size);
}

void ref_size2index_usize_fastpath(size_t size, unsigned * ind, size_t * usize)
{
    szind_t i;
    sz_size2index_usize_fastpath(size, &i, usize);
    *ind = i;
}

size_t ref_index2size_compute(unsigned index)
{
    return sz_index2size_compute(index);
}

size_t ref_index2size_unsafe(unsigned index)
{
    return sz_index2size_unsafe(index);
}

size_t ref_index2size(unsigned index)
{
    return sz_index2size(index);
}

size_t ref_s2u(size_t size)
{
    return sz_s2u(size);
}

size_t ref_s2u_compute(size_t size)
{
    return sz_s2u_compute(size);
}

size_t ref_sa2u(size_t size, size_t alignment)
{
    return sz_sa2u(size, alignment);
}

unsigned ref_psz2ind(size_t psz)
{
    return sz_psz2ind(psz);
}

size_t ref_pind2sz(unsigned pind)
{
    return sz_pind2sz(pind);
}

size_t ref_pind2sz_compute(unsigned pind)
{
    return sz_pind2sz_compute(pind);
}

size_t ref_psz2u(size_t psz)
{
    return sz_psz2u(psz);
}

size_t ref_psz_quantize_floor(size_t size)
{
    return sz_psz_quantize_floor(size);
}

size_t ref_psz_quantize_ceil(size_t size)
{
    return sz_psz_quantize_ceil(size);
}

bool ref_can_use_slab(size_t size)
{
    return sz_can_use_slab(size);
}

bool ref_large_size_classes_disabled(void)
{
    return sz_large_size_classes_disabled();
}

uint32_t ref_div_magic(size_t d)
{
    div_info_t info;
    div_init(&info, d);
    return info.magic;
}

size_t ref_div_compute(size_t d, size_t n)
{
    div_info_t info;
    div_init(&info, d);
    return div_compute(&info, n);
}

/* `lib_jemalloc.a` is built with `JEMALLOC_PROF_LIBUNWIND`; linking `jemalloc.o` (for `opt_disable_large_size_classes`
 * and the boot functions) pulls in the profiler, which is never activated here. */
__attribute__((weak)) int unw_backtrace(void ** buffer, int size)
{
    (void)buffer;
    (void)size;
    return 0;
}
