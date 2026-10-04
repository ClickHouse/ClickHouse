/* The sizes of jemalloc's ctl structures (`ctl.h`), which are allocated from `b0` and therefore visible in
 * `stats.metadata`. */

#ifndef _GNU_SOURCE
#    define _GNU_SOURCE
#endif

#include "jemalloc/internal/jemalloc_preamble.h"
#include "jemalloc/internal/jemalloc_internal_includes.h"

#include "jemalloc/internal/ctl.h"

size_t ref_sizeof_ctl_arena(void)
{
    return sizeof(ctl_arena_t);
}

size_t ref_sizeof_ctl_arenas(void)
{
    return sizeof(ctl_arenas_t);
}

size_t ref_sizeof_ctl_stats(void)
{
    return sizeof(ctl_stats_t);
}

size_t ref_sizeof_ctl_arena_stats(void)
{
    return sizeof(ctl_arena_stats_t);
}

int ref_opt_prof(void)
{
    return opt_prof;
}

int ref_opt_prof_stats(void)
{
    return opt_prof_stats;
}
