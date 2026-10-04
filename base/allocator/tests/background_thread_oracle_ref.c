/* Exposes the layout of jemalloc's background thread structures and a verbatim copy of the interval computation of
 * `background_work_sleep_once` (`background_thread.c`, static there) for `background_thread_oracle.cpp`. */

#ifndef _GNU_SOURCE
#    define _GNU_SOURCE
#endif

#include "jemalloc/internal/jemalloc_preamble.h"
#include "jemalloc/internal/jemalloc_internal_includes.h"

#include <stddef.h>

#define BILLION UINT64_C(1000000000)
#define BACKGROUND_THREAD_MIN_INTERVAL_NS (BILLION / 10)

void ref_background_thread_layout(size_t out[16])
{
    out[0] = sizeof(background_thread_info_t);
    out[1] = offsetof(background_thread_info_t, thread);
    out[2] = offsetof(background_thread_info_t, cond);
    out[3] = offsetof(background_thread_info_t, mtx);
    out[4] = offsetof(background_thread_info_t, state);
    out[5] = offsetof(background_thread_info_t, indefinite_sleep);
    out[6] = offsetof(background_thread_info_t, next_wakeup);
    out[7] = offsetof(background_thread_info_t, npages_to_purge_new);
    out[8] = offsetof(background_thread_info_t, tot_n_runs);
    out[9] = offsetof(background_thread_info_t, tot_sleep_time);
    out[10] = sizeof(background_thread_stats_t);
    out[11] = offsetof(background_thread_stats_t, max_counter_per_bg_thd);
    out[12] = BACKGROUND_THREAD_MIN_INTERVAL_NS;
    out[13] = DEFAULT_NUM_BACKGROUND_THREAD;
    out[14] = MAX_BACKGROUND_THREAD_LIMIT;
    out[15] = (size_t)background_thread_paused;
}

typedef struct
{
    const uint64_t * times; /* UINT64_MAX - 1 marks a missing arena */
    unsigned * worked;
    unsigned nworked;
    unsigned * queried;
    unsigned nqueried;
} ref_scan_t;

/* `background_work_sleep_once` without the sleep, with the arena accessors replaced by the script. */
uint64_t ref_background_work_sleep_ns(ref_scan_t * scan, unsigned ind, unsigned narenas, size_t max_threads, bool slept_indefinitely)
{
    uint64_t ns_until_deferred = BACKGROUND_THREAD_DEFERRED_MAX;

    for (unsigned i = ind; i < narenas; i += max_threads) {
        const uint64_t * arena = scan->times[i] == UINT64_MAX - 1 ? NULL : &scan->times[i];
        if (!arena) {
            continue;
        }
        if (!slept_indefinitely) {
            scan->worked[scan->nworked++] = i;
        }
        if (ns_until_deferred <= BACKGROUND_THREAD_MIN_INTERVAL_NS) {
            continue;
        }
        scan->queried[scan->nqueried++] = i;
        uint64_t ns_arena_deferred = *arena;
        if (ns_arena_deferred < ns_until_deferred) {
            ns_until_deferred = ns_arena_deferred;
        }
    }

    uint64_t sleep_ns;
    if (ns_until_deferred == BACKGROUND_THREAD_DEFERRED_MAX) {
        sleep_ns = BACKGROUND_THREAD_INDEFINITE_SLEEP;
    } else {
        sleep_ns = (ns_until_deferred < BACKGROUND_THREAD_MIN_INTERVAL_NS) ? BACKGROUND_THREAD_MIN_INTERVAL_NS
                                                                           : ns_until_deferred;
    }
    return sleep_ns;
}
