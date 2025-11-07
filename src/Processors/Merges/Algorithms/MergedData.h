#pragma once

#include <Processors/Chunk.h>
#include <Processors/Merges/Algorithms/IMergingAlgorithm.h>

namespace DB
{

class Block;

/// Class which represents current merging chunk of data.
/// Also it calculates the number of merged rows and other profile info.
class MergedData
{
public:
    explicit MergedData(bool use_average_block_size_, UInt64 max_block_size_, UInt64 max_block_size_bytes_, std::optional<size_t> max_dynamic_subcolumns_)
        : max_block_size(max_block_size_), max_block_size_bytes(max_block_size_bytes_), use_average_block_size(use_average_block_size_), max_dynamic_subcolumns(max_dynamic_subcolumns_)
    {
    }

    virtual void initialize(const Block & header, const IMergingAlgorithm::Inputs & inputs);

    /// Pull will be called at next prepare call.
    void flush() { need_flush = true; }

    void insertRow(const ColumnRawPtrs & raw_columns, size_t row, size_t block_size);

    void insertRows(const ColumnRawPtrs & raw_columns, size_t start_index, size_t length, size_t block_size);

    void insertChunk(Chunk && chunk, size_t rows_size);

    Chunk pull();

    bool hasEnoughRows() const;

    UInt64 mergedRows() const { return merged_rows; }
    UInt64 totalMergedRows() const { return total_merged_rows; }
    UInt64 totalChunks() const { return total_chunks; }
    UInt64 totalAllocatedBytes() const { return total_allocated_bytes; }
    UInt64 maxBlockSize() const { return max_block_size; }

    IMergingAlgorithm::MergedStats getMergedStats() const { return {.bytes = total_allocated_bytes, .rows = total_merged_rows, .blocks = total_chunks}; }

    virtual ~MergedData() = default;

protected:
    MutableColumns columns;

    UInt64 sum_blocks_granularity = 0;
    UInt64 merged_rows = 0;
    UInt64 total_merged_rows = 0;
    UInt64 total_chunks = 0;
    UInt64 total_allocated_bytes = 0;

    const UInt64 max_block_size = 0;
    const UInt64 max_block_size_bytes = 0;
    const bool use_average_block_size = false;
    const std::optional<size_t> max_dynamic_subcolumns;

    bool need_flush = false;
};

}
