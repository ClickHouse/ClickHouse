/* Exposes jemalloc's `exp_grow_*` (`exp_grow.h`, `src/exp_grow.c`) to `exp_grow_oracle.cpp`. */

#ifndef _GNU_SOURCE
#    define _GNU_SOURCE
#endif

#include "jemalloc/internal/jemalloc_preamble.h"
#include "jemalloc/internal/jemalloc_internal_includes.h"

#include "jemalloc/internal/exp_grow.h"
#include "jemalloc/internal/sc.h"
#include "jemalloc/internal/sz.h"

static sc_data_t ref_sc_data;

/* `sz_pind2sz` reads `sz_pind2sz_tab`, which is filled by `sz_boot`. */
void ref_exp_grow_boot(void)
{
    sc_boot(&ref_sc_data);
    sz_boot(&ref_sc_data, true);
}

void ref_exp_grow_init(unsigned * next, unsigned * limit)
{
    exp_grow_t eg;
    exp_grow_init(&eg);
    *next = eg.next;
    *limit = eg.limit;
}

bool ref_exp_grow_size_prepare(unsigned next, unsigned limit, size_t alloc_size_min, size_t * r_alloc_size, unsigned * r_skip)
{
    exp_grow_t eg = {next, limit};
    return exp_grow_size_prepare(&eg, alloc_size_min, r_alloc_size, r_skip);
}

unsigned ref_exp_grow_size_commit(unsigned next, unsigned limit, unsigned skip)
{
    exp_grow_t eg = {next, limit};
    exp_grow_size_commit(&eg, skip);
    return eg.next;
}
