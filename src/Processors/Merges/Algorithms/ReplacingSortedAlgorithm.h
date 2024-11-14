#pragma once
#include <Processors/Merges/Algorithms/IMergingAlgorithmWithSharedChunks.h>
#include <Processors/Merges/Algorithms/MergedData.h>
#include <Processors/Transforms/ColumnGathererTransform.h>
#include <Processors/Merges/Algorithms/RowRef.h>
#include <Processors/Chunk.h>

namespace Poco
{
class Logger;
}

namespace DB
{

//// Used in skipping final to keep the list of indices of selected rows after merging.
struct ChunkSelectFinalIndices : public ChunkInfoCloneable<ChunkSelectFinalIndices>
{
    explicit ChunkSelectFinalIndices(MutableColumnPtr select_final_indices_);
    ChunkSelectFinalIndices(const ChunkSelectFinalIndices & other) = default;

    const ColumnPtr column_holder;
    const ColumnUInt64 * select_final_indices = nullptr;
};

//// Used in skipping final to keep all rows in chunk after merging.
struct ChunkSelectFinalAllRows : public ChunkInfoCloneable<ChunkSelectFinalAllRows>
{
};

/** Merges several sorted inputs into one.
  * For each group of consecutive identical values of the primary key (the columns by which the data is sorted),
  *  keeps row with max `version` value.
  */
class ReplacingSortedAlgorithm final : public IMergingAlgorithmWithSharedChunks
{
public:
    ReplacingSortedAlgorithm(
        const Block & header, size_t num_inputs,
        SortDescription description_,
        const String & is_deleted_column,
        const String & version_column,
        size_t max_block_size_rows,
        size_t max_block_size_bytes,
        WriteBuffer * out_row_sources_buf_ = nullptr,
        bool use_average_block_sizes = false,
        bool cleanup = false,
        bool enable_vertical_final_ = false);

    const char * getName() const override { return "ReplacingSortedAlgorithm"; }
    Status merge() override;

private:
    ssize_t is_deleted_column_number = -1;
    ssize_t version_column_number = -1;
    bool cleanup = false;

    bool enable_vertical_final = false; /// Either we use skipping final algorithm
    std::queue<detail::SharedChunkPtr> to_be_emitted;   /// To save chunks when using skipping final

    using RowRef = detail::RowRefWithOwnedChunk;
    static constexpr size_t max_row_refs = 2; /// last, current.
    RowRef selected_row; /// Last row with maximum version for current primary key, may extend lifetime of chunk in input source
    size_t max_pos = 0; /// The position (into current_row_sources) of the row with the highest version.

    /// Sources of rows with the current primary key.
    PODArray<RowSourcePart> current_row_sources;

    void insertRow();
    void insertRowImpl();

    /// Method for using in skipping FINAL logic
    /// Skipping FINAL doesn't merge rows to new chunks but marks selected rows in input chunks and emit them

    /// When removing a source from queue, if source 's chunk has selected rows then we keep it
    void saveChunkForSkippingFinalFromSource(size_t current_source_index);
    /// When changing current `selected_row`, if selected_row is the last owner of an input chunk and that chunk
    /// has selected rows, we also keep it
    void saveChunkForSkippingFinalFromSelectedRow();
};

}
