/* Exposes jemalloc's `exp_grow_*` (`exp_grow.h`, `src/exp_grow.c`) to `exp_grow_oracle.cpp`. */

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include "jemalloc/internal/jemalloc_internal_includes.h"
#include "jemalloc/internal/jemalloc_preamble.h"

#include "jemalloc/internal/exp_grow.h"
#include "jemalloc/internal/sc.h"
#include "jemalloc/internal/sz.h"

static sc_data_t ref_size_class_data;

/* `sz_pind2sz` reads `sz_pind2sz_tab`, which is filled by `sz_boot`. */
void ref_exponential_grow_boot(void)
{
    sc_boot(&ref_size_class_data);
    sz_boot(&ref_size_class_data, true);
}

void ref_exponential_grow_init(unsigned * next, unsigned * limit)
{
    exp_grow_t exponential_grow;
    exp_grow_init(&exponential_grow);
    *next = exponential_grow.next;
    *limit = exponential_grow.limit;
}

bool ref_exponential_grow_size_prepare(
    unsigned next, unsigned limit, size_t alloc_size_min, size_t * result_alloc_size, unsigned * result_skip)
{
    exp_grow_t exponential_grow = {next, limit};
    return exp_grow_size_prepare(&exponential_grow, alloc_size_min, result_alloc_size, result_skip);
}

unsigned ref_exponential_grow_size_commit(unsigned next, unsigned limit, unsigned skip)
{
    exp_grow_t exponential_grow = {next, limit};
    exp_grow_size_commit(&exponential_grow, skip);
    return exponential_grow.next;
}
