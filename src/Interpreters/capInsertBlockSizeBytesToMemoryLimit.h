#pragma once

#include <cstddef>

namespace DB
{

/// Caps `min_insert_block_size_bytes` (or a value playing its role) so that the several block-sized
/// copies a writing pipeline holds at once stay a bounded share of the server's memory limit. Shared
/// between the direct `INSERT` pipeline, the dependent materialized-view pipelines and the projection
/// squashes of merges and mutations, so the cap is applied symmetrically. `num_simultaneous_squashes` is
/// the number of such squashing pipelines the caller's operation keeps alive at once; they share the
/// budget. See the definition for the memory model behind it.
size_t capInsertBlockSizeBytesToMemoryLimit(size_t min_block_size_bytes, size_t num_simultaneous_squashes);

}
