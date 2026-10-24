#ifndef DATA_POOL_H
#define DATA_POOL_H

#include "maxminddb.h"

#include <stdbool.h>
#include <stddef.h>

// Keep the block array fixed so that its own growth does not need a rarely used
// reallocation path. Even starting with one struct, 32 geometrically growing
// blocks cover every practical allocation; the last block may be clamped to the
// configured capacity.
#define DATA_POOL_NUM_BLOCKS 32

// A pool of memory for MMDB_entry_data_list_s structs. This is so we can
// allocate multiple up front rather than one at a time for performance
// reasons.
//
// The order you add elements to it (by calling data_pool_alloc()) ends up as
// the order of the list.
//
// The memory only grows. There is no support for releasing an element you take
// back to the pool.
typedef struct MMDB_data_pool_s {
    // Index of the current block we're allocating out of.
    size_t index;

    // The size of the current block, counting by structs.
    size_t size;

    // How many used in the current block, counting by structs.
    size_t used;

    // Total number of structs reserved across all blocks.
    size_t capacity;

    // Maximum total number of structs this pool may reserve.
    size_t max_size;

    // The current block we're allocating out of.
    MMDB_entry_data_list_s *block;

    // The size of each block.
    size_t sizes[DATA_POOL_NUM_BLOCKS];

    // An array of pointers to blocks of memory holding space for list
    // elements.
    MMDB_entry_data_list_s *blocks[DATA_POOL_NUM_BLOCKS];
} MMDB_data_pool_s;

bool can_multiply(size_t const, size_t const, size_t const);
MMDB_data_pool_s *data_pool_new(size_t const, size_t const);
void data_pool_destroy(MMDB_data_pool_s *const);
MMDB_entry_data_list_s *data_pool_alloc(MMDB_data_pool_s *const);
MMDB_entry_data_list_s *data_pool_to_list(MMDB_data_pool_s *const);

#endif
