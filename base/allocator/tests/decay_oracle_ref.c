/* Wraps jemalloc's `decay_*` functions (exported unprefixed from `lib_jemalloc.a`) for `decay_oracle.cpp`. */

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include "jemalloc/internal/jemalloc_internal_includes.h"
#include "jemalloc/internal/jemalloc_preamble.h"

#include "jemalloc/internal/decay.h"

#include "decay_oracle_ref.h"

static const uint64_t ref_h_steps[SMOOTHSTEP_NSTEPS] = {
#define STEP(step, h, x, y) h,
    SMOOTHSTEP
#undef STEP
};

void ref_decay_layout(size_t out[REF_DECAY_LAYOUT_SIZE])
{
    out[REF_DECAY_SIZEOF] = sizeof(decay_t);
    out[REF_DECAY_OFFSET_PURGING] = offsetof(decay_t, purging);
    out[REF_DECAY_OFFSET_TIME_MS] = offsetof(decay_t, time_ms);
    out[REF_DECAY_OFFSET_INTERVAL] = offsetof(decay_t, interval);
    out[REF_DECAY_OFFSET_EPOCH] = offsetof(decay_t, epoch);
    out[REF_DECAY_OFFSET_JITTER_STATE] = offsetof(decay_t, jitter_state);
    out[REF_DECAY_OFFSET_DEADLINE] = offsetof(decay_t, deadline);
    out[REF_DECAY_OFFSET_NUM_PAGES_LIMIT] = offsetof(decay_t, npages_limit);
    out[REF_DECAY_OFFSET_NUM_UNPURGED] = offsetof(decay_t, nunpurged);
    out[REF_DECAY_OFFSET_BACKLOG] = offsetof(decay_t, backlog);
    out[REF_DECAY_OFFSET_CEIL_NUM_PAGES] = offsetof(decay_t, ceil_npages);
}

uint64_t ref_h_step(unsigned i)
{
    return ref_h_steps[i];
}

unsigned ref_smoothstep_num_steps(void)
{
    return SMOOTHSTEP_NSTEPS;
}

unsigned ref_smoothstep_binary_fixed_point(void)
{
    return SMOOTHSTEP_BFP;
}

bool ref_decay_ms_valid(ssize_t decay_ms)
{
    return decay_ms_valid(decay_ms);
}

bool ref_decay_init(void * memory, uint64_t current_ns, ssize_t decay_ms)
{
    nstime_t t;
    nstime_init(&t, current_ns);
    return decay_init((decay_t *)memory, &t, decay_ms);
}

void ref_decay_reinit(void * memory, uint64_t current_ns, ssize_t decay_ms)
{
    nstime_t t;
    nstime_init(&t, current_ns);
    decay_reinit((decay_t *)memory, &t, decay_ms);
}

bool ref_decay_maybe_advance_epoch(void * memory, uint64_t new_ns, size_t num_pages_current)
{
    nstime_t t;
    nstime_init(&t, new_ns);
    return decay_maybe_advance_epoch((decay_t *)memory, &t, num_pages_current);
}

uint64_t ref_decay_num_pages_purge_in(void * memory, uint64_t time_ns, size_t num_pages_new)
{
    nstime_t t;
    nstime_init(&t, time_ns);
    return decay_npages_purge_in((decay_t *)memory, &t, num_pages_new);
}

uint64_t ref_decay_ns_until_purge(void * memory, size_t num_pages_current, uint64_t num_pages_threshold)
{
    return decay_ns_until_purge((decay_t *)memory, num_pages_current, num_pages_threshold);
}

void ref_decay_state(const void * memory, struct RefDecayState * state)
{
    const decay_t * decay = (const decay_t *)memory;
    state->time_ms = decay_ms_read(decay);
    state->interval = nstime_ns(&decay->interval);
    state->epoch = nstime_ns(&decay->epoch);
    state->jitter_state = decay->jitter_state;
    state->deadline = nstime_ns(&decay->deadline);
    state->num_pages_limit = decay_npages_limit_get(decay);
    state->num_unpurged = decay->nunpurged;
    for (unsigned i = 0; i < SMOOTHSTEP_NSTEPS; i++)
        state->backlog[i] = decay->backlog[i];
    state->purging = decay->purging;
}

bool ref_decay_queries(const void * memory, unsigned which)
{
    const decay_t * decay = (const decay_t *)memory;
    switch (which)
    {
        case 0: return decay_immediately(decay);
        case 1: return decay_disabled(decay);
        case 2: return decay_gradually(decay);
        default: return decay_epoch_npages_delta(decay) != 0;
    }
}
