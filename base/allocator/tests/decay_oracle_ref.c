/* Wraps jemalloc's `decay_*` functions (exported unprefixed from `lib_jemalloc.a`) for `decay_oracle.cpp`. */

#ifndef _GNU_SOURCE
#    define _GNU_SOURCE
#endif

#include "jemalloc/internal/jemalloc_preamble.h"
#include "jemalloc/internal/jemalloc_internal_includes.h"

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
    out[REF_DECAY_OFFSET_NPAGES_LIMIT] = offsetof(decay_t, npages_limit);
    out[REF_DECAY_OFFSET_NUNPURGED] = offsetof(decay_t, nunpurged);
    out[REF_DECAY_OFFSET_BACKLOG] = offsetof(decay_t, backlog);
    out[REF_DECAY_OFFSET_CEIL_NPAGES] = offsetof(decay_t, ceil_npages);
}

uint64_t ref_h_step(unsigned i)
{
    return ref_h_steps[i];
}

unsigned ref_smoothstep_nsteps(void)
{
    return SMOOTHSTEP_NSTEPS;
}

unsigned ref_smoothstep_bfp(void)
{
    return SMOOTHSTEP_BFP;
}

bool ref_decay_ms_valid(ssize_t decay_ms)
{
    return decay_ms_valid(decay_ms);
}

bool ref_decay_init(void * mem, uint64_t cur_ns, ssize_t decay_ms)
{
    nstime_t t;
    nstime_init(&t, cur_ns);
    return decay_init((decay_t *)mem, &t, decay_ms);
}

void ref_decay_reinit(void * mem, uint64_t cur_ns, ssize_t decay_ms)
{
    nstime_t t;
    nstime_init(&t, cur_ns);
    decay_reinit((decay_t *)mem, &t, decay_ms);
}

bool ref_decay_maybe_advance_epoch(void * mem, uint64_t new_ns, size_t npages_current)
{
    nstime_t t;
    nstime_init(&t, new_ns);
    return decay_maybe_advance_epoch((decay_t *)mem, &t, npages_current);
}

uint64_t ref_decay_npages_purge_in(void * mem, uint64_t time_ns, size_t npages_new)
{
    nstime_t t;
    nstime_init(&t, time_ns);
    return decay_npages_purge_in((decay_t *)mem, &t, npages_new);
}

uint64_t ref_decay_ns_until_purge(void * mem, size_t npages_current, uint64_t npages_threshold)
{
    return decay_ns_until_purge((decay_t *)mem, npages_current, npages_threshold);
}

void ref_decay_state(const void * mem, struct RefDecayState * state)
{
    const decay_t * decay = (const decay_t *)mem;
    state->time_ms = decay_ms_read(decay);
    state->interval = nstime_ns(&decay->interval);
    state->epoch = nstime_ns(&decay->epoch);
    state->jitter_state = decay->jitter_state;
    state->deadline = nstime_ns(&decay->deadline);
    state->npages_limit = decay_npages_limit_get(decay);
    state->nunpurged = decay->nunpurged;
    for (unsigned i = 0; i < SMOOTHSTEP_NSTEPS; i++)
        state->backlog[i] = decay->backlog[i];
    state->purging = decay->purging;
}

bool ref_decay_queries(const void * mem, unsigned which)
{
    const decay_t * decay = (const decay_t *)mem;
    switch (which)
    {
        case 0: return decay_immediately(decay);
        case 1: return decay_disabled(decay);
        case 2: return decay_gradually(decay);
        default: return decay_epoch_npages_delta(decay) != 0;
    }
}
