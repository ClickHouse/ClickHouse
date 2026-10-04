/* Shared between `decay_oracle.cpp` and `decay_oracle_ref.c`. */

#pragma once

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#define REF_DECAY_NSTEPS 200

struct RefDecayState
{
    int64_t time_ms;
    uint64_t interval;
    uint64_t epoch;
    uint64_t jitter_state;
    uint64_t deadline;
    uint64_t npages_limit;
    uint64_t nunpurged;
    uint64_t backlog[REF_DECAY_NSTEPS];
    bool purging;
};

/* Indices into the array filled by `ref_decay_layout`. */
enum
{
    REF_DECAY_SIZEOF,
    REF_DECAY_OFFSET_PURGING,
    REF_DECAY_OFFSET_TIME_MS,
    REF_DECAY_OFFSET_INTERVAL,
    REF_DECAY_OFFSET_EPOCH,
    REF_DECAY_OFFSET_JITTER_STATE,
    REF_DECAY_OFFSET_DEADLINE,
    REF_DECAY_OFFSET_NPAGES_LIMIT,
    REF_DECAY_OFFSET_NUNPURGED,
    REF_DECAY_OFFSET_BACKLOG,
    REF_DECAY_OFFSET_CEIL_NPAGES,
    REF_DECAY_LAYOUT_SIZE,
};

#ifdef __cplusplus
extern "C"
{
#endif

void ref_decay_layout(size_t out[REF_DECAY_LAYOUT_SIZE]);
uint64_t ref_h_step(unsigned i);
unsigned ref_smoothstep_nsteps(void);
unsigned ref_smoothstep_bfp(void);
bool ref_decay_ms_valid(ssize_t decay_ms);
/* `mem` must be zeroed and have room for a `decay_t`. */
bool ref_decay_init(void * mem, uint64_t cur_ns, ssize_t decay_ms);
void ref_decay_reinit(void * mem, uint64_t cur_ns, ssize_t decay_ms);
bool ref_decay_maybe_advance_epoch(void * mem, uint64_t new_ns, size_t npages_current);
uint64_t ref_decay_npages_purge_in(void * mem, uint64_t time_ns, size_t npages_new);
uint64_t ref_decay_ns_until_purge(void * mem, size_t npages_current, uint64_t npages_threshold);
void ref_decay_state(const void * mem, struct RefDecayState * state);
bool ref_decay_queries(const void * mem, unsigned which);

#ifdef __cplusplus
}
#endif
