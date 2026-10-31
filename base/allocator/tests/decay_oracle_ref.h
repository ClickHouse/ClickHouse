/* Shared between `decay_oracle.cpp` and `decay_oracle_ref.c`. */

#pragma once

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#define REF_DECAY_NUM_STEPS 200

struct RefDecayState
{
    int64_t time_ms;
    uint64_t interval;
    uint64_t epoch;
    uint64_t jitter_state;
    uint64_t deadline;
    uint64_t num_pages_limit;
    uint64_t num_unpurged;
    uint64_t backlog[REF_DECAY_NUM_STEPS];
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
    REF_DECAY_OFFSET_NUM_PAGES_LIMIT,
    REF_DECAY_OFFSET_NUM_UNPURGED,
    REF_DECAY_OFFSET_BACKLOG,
    REF_DECAY_OFFSET_CEIL_NUM_PAGES,
    REF_DECAY_LAYOUT_SIZE,
};

#ifdef __cplusplus
extern "C" {
#endif

void ref_decay_layout(size_t out[REF_DECAY_LAYOUT_SIZE]);
uint64_t ref_h_step(unsigned i);
unsigned ref_smoothstep_num_steps(void);
unsigned ref_smoothstep_binary_fixed_point(void);
bool ref_decay_ms_valid(ssize_t decay_ms);
/* `memory` must be zeroed and have room for a `decay_t`. */
bool ref_decay_init(void * memory, uint64_t current_ns, ssize_t decay_ms);
void ref_decay_reinit(void * memory, uint64_t current_ns, ssize_t decay_ms);
bool ref_decay_maybe_advance_epoch(void * memory, uint64_t new_ns, size_t num_pages_current);
uint64_t ref_decay_num_pages_purge_in(void * memory, uint64_t time_ns, size_t num_pages_new);
uint64_t ref_decay_ns_until_purge(void * memory, size_t num_pages_current, uint64_t num_pages_threshold);
void ref_decay_state(const void * memory, struct RefDecayState * state);
bool ref_decay_queries(const void * memory, unsigned which);

#ifdef __cplusplus
}
#endif
