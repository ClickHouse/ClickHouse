#pragma once

#include <Processors/Chunk.h>
#include <Processors/Merges/Algorithms/IMergingAlgorithm.h>

namespace DB
{

class Block;

/// Description of one output chunk whose columns still need to be materialized.
/// Sources own each input column bundle once; runs refer to them by index so that
/// short, interleaved ranges do not copy all `ColumnPtr` objects per run.
struct MergedDataMaterializationInfo final : ChunkInfoCloneable<MergedDataMaterializationInfo>
{
    struct Source
    {
        Columns columns;
        size_t num_rows = 0;
    };

    struct Run
    {
        size_t source = 0;
        size_t start = 0;
        size_t length = 0;
    };

    Columns output_columns;
    std::vector<Source> sources;
    std::vector<Run> runs;
};

/// Class which represents current merging chunk of data.
/// Also it calculates the number of merged rows and other profile info.
class MergedData
{
public:
    explicit MergedData(
        bool use_average_block_size_,
        UInt64 max_block_size_,
        UInt64 max_block_size_bytes_,
        std::optional<size_t> max_dynamic_subcolumns_,
        bool defer_materialization_ = false)
        : max_block_size(max_block_size_)
        , max_block_size_bytes(max_block_size_bytes_)
        , use_average_block_size(use_average_block_size_)
        , max_dynamic_subcolumns(max_dynamic_subcolumns_)
        , defer_materialization(defer_materialization_)
    {
    }

    virtual void initialize(const Block & header, const IMergingAlgorithm::Inputs & inputs);

    /// Initialize an ordinary `MergedData` from the output-column prototypes saved in
    /// a deferred materialization plan.
    void initializeFromColumns(const Columns & prototype_columns);

    /// Register the current chunk of an input source. It is retained in a plan only
    /// if at least one range from it is selected for that output chunk.
    void setSource(size_t source_num, const Columns & source_columns, size_t num_rows);

    /// Pull will be called at next prepare call.
    void flush() { need_flush = true; }

    void insertRow(const ColumnRawPtrs & raw_columns, size_t row, size_t block_size);

    void insertRows(const ColumnRawPtrs & raw_columns, size_t start_index, size_t length, size_t block_size);

    void insertRowFromSource(size_t source_num, const ColumnRawPtrs & raw_columns, size_t row, size_t block_size);

    void insertRowsFromSource(
        size_t source_num,
        const ColumnRawPtrs & raw_columns,
        size_t start_index,
        size_t length,
        size_t block_size);

    void insertChunk(Chunk && chunk, size_t rows_size);

    void insertChunkFromSource(size_t source_num, Chunk && chunk, size_t rows_size);

    Chunk pull();

    bool hasEnoughRows() const;

    size_t rowsToInsertBeforeFlush(
        const ColumnRawPtrs & raw_columns,
        size_t start_index,
        size_t max_rows,
        size_t block_size) const;

    UInt64 mergedRows() const { return merged_rows; }
    UInt64 totalMergedRows() const { return total_merged_rows; }
    UInt64 totalChunks() const { return total_chunks; }
    UInt64 totalAllocatedBytes() const { return total_allocated_bytes; }
    UInt64 maxBlockSize() const { return max_block_size; }
    bool isDeferredMaterialization() const { return defer_materialization; }

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
    const bool defer_materialization = false;

    bool need_flush = false;

private:
    size_t getOrAddMaterializationSource(size_t source_num, bool release_source);
    void appendMaterializationRun(size_t source, size_t start, size_t length);

    std::vector<MergedDataMaterializationInfo::Source> current_materialization_sources;
    std::vector<bool> current_materialization_source_is_set;
    std::vector<std::optional<size_t>> materialization_source_handles;
    std::vector<MergedDataMaterializationInfo::Source> materialization_sources;
    std::vector<MergedDataMaterializationInfo::Run> materialization_runs;
};

}
