#pragma once

#include <cstddef>

namespace DB
{

struct Settings;

/// Caps `min_insert_block_size_bytes` (or a value playing its role) by `insert_squashing_max_memory_ratio`
/// of the server's memory limit. Shared between the direct `INSERT` pipeline and the pipelines of the
/// dependent materialized views, so the cap is applied the same way everywhere.
size_t capInsertBlockSizeBytesToMemoryLimit(size_t min_block_size_bytes, const Settings & settings);

}
