#ifndef _POSIX_C_SOURCE
    #define _POSIX_C_SOURCE 200809L
#endif

#include "data-pool.h"
#include "maxminddb.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdlib.h>

// Allocate an MMDB_data_pool_s. It initially has space for up to size
// MMDB_entry_data_list_s structs. Its total capacity will not exceed max_size.
MMDB_data_pool_s *data_pool_new(size_t const size, size_t const max_size) {
    MMDB_data_pool_s *const pool = calloc(1, sizeof(MMDB_data_pool_s));
    if (!pool) {
        return NULL;
    }

    if (size == 0 || max_size == 0) {
        data_pool_destroy(pool);
        return NULL;
    }
    size_t initial_size = size;
    if (initial_size > max_size) {
        initial_size = max_size;
    }
    if (!can_multiply(SIZE_MAX, initial_size, sizeof(MMDB_entry_data_list_s))) {
        data_pool_destroy(pool);
        return NULL;
    }
    pool->size = initial_size;
    pool->blocks[0] = calloc(pool->size, sizeof(MMDB_entry_data_list_s));
    if (!pool->blocks[0]) {
        data_pool_destroy(pool);
        return NULL;
    }
    pool->blocks[0]->pool = pool;

    pool->sizes[0] = initial_size;
    pool->capacity = initial_size;
    pool->max_size = max_size;

    pool->block = pool->blocks[0];

    return pool;
}

// Determine if we can multiply m*n. We can do this if the result will be below
// the given max. max will typically be SIZE_MAX.
//
// We want to know if we'll wrap around.
bool can_multiply(size_t const max, size_t const m, size_t const n) {
    if (m == 0) {
        return false;
    }

    return n <= max / m;
}

// Clean up the data pool.
void data_pool_destroy(MMDB_data_pool_s *const pool) {
    if (!pool) {
        return;
    }

    for (size_t i = 0; i <= pool->index; i++) {
        free(pool->blocks[i]);
    }

    free(pool);
}

// Claim a new struct from the pool. Doing this may grow the pool. NULL means an
// allocation failed or the pool reached its maximum capacity. Decoder callers
// check their logical limit before reaching the capacity limit.
MMDB_entry_data_list_s *data_pool_alloc(MMDB_data_pool_s *const pool) {
    if (!pool) {
        return NULL;
    }

    if (pool->used < pool->size) {
        MMDB_entry_data_list_s *const element = pool->block + pool->used;
        pool->used++;
        return element;
    }

    if (pool->capacity == pool->max_size) {
        return NULL;
    }

    // Take it from a new block of memory.

    size_t const new_index = pool->index + 1;
    if (new_index == DATA_POOL_NUM_BLOCKS) {
        // See the comment about not growing this on DATA_POOL_NUM_BLOCKS.
        return NULL;
    }

    size_t const remaining = pool->max_size - pool->capacity;
    size_t new_size = remaining;
    if (pool->size <= remaining / 2) {
        new_size = pool->size * 2;
    }

    if (!can_multiply(SIZE_MAX, new_size, sizeof(MMDB_entry_data_list_s))) {
        return NULL;
    }
    pool->blocks[new_index] = calloc(new_size, sizeof(MMDB_entry_data_list_s));
    if (!pool->blocks[new_index]) {
        return NULL;
    }

    // We don't need to set this, but it's useful for introspection in tests.
    pool->blocks[new_index]->pool = pool;

    pool->index = new_index;
    pool->block = pool->blocks[pool->index];

    pool->size = new_size;
    pool->sizes[pool->index] = pool->size;
    pool->capacity += new_size;

    MMDB_entry_data_list_s *const element = pool->block;
    pool->used = 1;
    return element;
}

// Turn the structs in the array-like pool into a linked list.
//
// Before calling this function, the list isn't linked up.
MMDB_entry_data_list_s *data_pool_to_list(MMDB_data_pool_s *const pool) {
    if (!pool) {
        return NULL;
    }

    if (pool->index == 0 && pool->used == 0) {
        return NULL;
    }

    for (size_t i = 0; i <= pool->index; i++) {
        MMDB_entry_data_list_s *const block = pool->blocks[i];

        size_t size = pool->sizes[i];
        if (i == pool->index) {
            size = pool->used;
        }

        for (size_t j = 0; j < size - 1; j++) {
            MMDB_entry_data_list_s *const cur = block + j;
            cur->next = block + j + 1;
        }

        if (i < pool->index) {
            MMDB_entry_data_list_s *const last = block + size - 1;
            last->next = pool->blocks[i + 1];
        }
    }

    return pool->blocks[0];
}

#ifdef TEST_DATA_POOL

    #include <libtap/tap.h>
    #include <maxminddb_test_helper.h>

static void test_can_multiply(void);

int main(void) {
    plan(NO_PLAN);
    test_can_multiply();
    done_testing();
}

static void test_can_multiply(void) {
    {
        ok(can_multiply(SIZE_MAX, 1, SIZE_MAX), "1*SIZE_MAX is ok");
    }

    {
        ok(!can_multiply(SIZE_MAX, 2, SIZE_MAX), "2*SIZE_MAX is not ok");
    }

    {
        ok(can_multiply(SIZE_MAX, 10240, sizeof(MMDB_entry_data_list_s)),
           "1024 entry_data_list_s's are okay");
    }
}

#endif
