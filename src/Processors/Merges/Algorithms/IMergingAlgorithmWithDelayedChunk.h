#pragma once

#include <Processors/Merges/Algorithms/IMergingAlgorithm.h>
#include <Processors/Merges/Algorithms/RowRef.h>
#include <Core/SortDescription.h>

namespace DB
{

class IMergingAlgorithmWithDelayedChunk : public IMergingAlgorithm
{
public:
    IMergingAlgorithmWithDelayedChunk(
        size_t num_inputs,
        SortDescription description_);

protected:
    SortingHeap<SortCursor> queue;
    SortDescription description;

    /// Previous row. May refer to last_chunk_sort_columns or row from source_chunks.
    detail::RowRef last_key;

    ColumnRawPtrs last_chunk_sort_columns; /// Point to last_chunk if valid.

    void initializeQueue(Inputs inputs);
    void updateCursor(Input & input, size_t source_num);
    bool skipLastRowFor(size_t input_number) const { return current_inputs[input_number].skip_last_row; }

private:
    /// Inputs currently being merged.
    Inputs current_inputs;
    SortCursorImpls cursors;

    /// In merging algorithm, we need to compare current sort key with the last one.
    /// So, sorting columns for last row needed to be stored.
    /// In order to do it, we extend lifetime of last chunk and it's sort columns (from corresponding sort cursor).
    Chunk last_chunk;
};

}
