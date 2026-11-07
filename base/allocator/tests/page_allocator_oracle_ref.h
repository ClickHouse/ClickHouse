/* The interface of page_allocator_oracle_ref.c (shared by the C reference and the C++ test). */

#pragma once

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <sys/types.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct
{
    uintptr_t addr;
    size_t size;
    uint64_t serial_number;
    unsigned state;
    unsigned size_class_idx;
    unsigned arena_idx;
    bool zeroed;
    bool committed;
    bool guarded;
    bool slab;
    bool is_head;
} RefExtentInfo;

int ref_boot(void);
void ref_set_disable_large_size_classes(bool value);
bool ref_background_thread_enabled(void);
void ref_set_background_thread_enabled(bool value);
size_t ref_sizeof_page_allocator_shard(void);
size_t ref_sizeof_page_allocator(void);
void * ref_shard_new(unsigned idx, ssize_t dirty_ms, ssize_t muzzy_ms, size_t oversize_threshold);
void ref_shard_destroy(void * shard);
void * ref_alloc(void * shard, size_t size, size_t alignment, bool slab, unsigned size_class_idx, bool zero, bool guarded, bool * deferred);
bool ref_expand(void * shard, void * extent, size_t old_size, size_t new_size, unsigned size_class_idx, bool zero, bool * deferred);
bool ref_shrink(void * shard, void * extent, size_t old_size, size_t new_size, unsigned size_class_idx, bool * deferred);
void ref_deallocate(void * shard, void * extent, bool * deferred);
bool ref_maybe_decay_purge(void * shard, int which, int eagerness);
void ref_decay_all(void * shard, int which, bool fully_decay);
bool ref_decay_ms_set(void * shard, int which, ssize_t ms, int eagerness);
ssize_t ref_decay_ms_get(void * shard, int which);
void ref_decay_state(void * shard, int which, uint64_t * epoch_ns, size_t * num_pages_limit, size_t * num_unpurged, size_t * backlog_last);
uint64_t ref_time_until_deferred_work(void * shard);
bool ref_retain_grow_limit(void * shard, size_t * old_limit, size_t * new_limit);
void ref_extent_info(void * extent, RefExtentInfo * info);
size_t ref_extent_cache_list(void * shard, int which, bool guarded, RefExtentInfo * out, size_t max);
void ref_stats(void * shard, uint64_t * out);
size_t ref_num_stats(void);
void * ref_extent_map_lookup(void * shard, const void * ptr, unsigned * size_class_idx, bool * slab);

#ifdef __cplusplus
}
#endif

#ifdef __cplusplus
extern "C" {
#endif

void ref_trace_set_side(int side);
void ref_clock_set(uint64_t ns);
uint64_t ref_clock_get(void);
bool ref_normalize(int side, uintptr_t addr, size_t * rank, size_t * offset);
size_t ref_num_regions_of(int side);

#ifdef __cplusplus
}
#endif
