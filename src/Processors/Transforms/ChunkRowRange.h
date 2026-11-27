#pragma once

#include <Columns/IColumn.h>
#include <Processors/Chunk.h>

#include <vector>

namespace DB
{

/// Chunk-local half-open range of rows `[start, start + length)`.
struct ChunkRowRange
{
    UInt64 start = 0;
    UInt64 length = 0;
};

/// Materializes chunk-local `slices` from one source `Columns` into `chunk` and returns the output row
/// count. `slices` must be non-empty, ordered by `start`, non-overlapping, and each slice must stay within
/// `[0, source_row_count)`. The source columns are reused when every row survives; otherwise one contiguous
/// span is taken with a single `cut`, and a sparse selection falls back to a mask-based `filter`.
UInt64 materializeSlicesIntoChunk(Chunk & chunk, Columns && source_columns, UInt64 source_row_count, const std::vector<ChunkRowRange> & slices);

}
