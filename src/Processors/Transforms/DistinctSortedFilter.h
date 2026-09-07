#pragma once

#include <Core/ColumnNumbers.h>
#include <Core/SortDescription.h>
#include <Processors/Chunk.h>

namespace DB
{

/// Deduplicates a stream sorted by the distinct keys using a description without collators. It keeps the
/// first row of each equal range, including ranges split across chunks. Sort equality collapses signed
/// zeros and NaNs with different payloads, as it does for the in-order `DISTINCT` algorithm.
///
/// Each row carries a `UInt8` already-emitted flag. Flagged rows must precede unflagged rows with
/// equal keys. A flagged first row suppresses the entire range because its key was already emitted.
class DistinctSortedFilter
{
public:
    DistinctSortedFilter(ColumnNumbers key_columns_pos_, SortDescription description_, size_t flag_column_pos_);

    /// Keeps the first row of each equal-key range unless the range starts with an already-emitted
    /// flag, and removes the flag column. The result can be empty.
    Chunk filter(Chunk chunk);

private:
    void saveLatestKey(const ColumnRawPtrs & key_columns, size_t row_pos);
    bool isLatestKeyFromPrevChunk(const ColumnRawPtrs & key_columns, size_t row_pos) const;

    const ColumnNumbers key_columns_pos;
    const SortDescription description;
    const size_t flag_column_pos;

    /// The latest key detects an equal range that continues into the next chunk.
    MutableColumns prev_chunk_latest_key;
};

}
