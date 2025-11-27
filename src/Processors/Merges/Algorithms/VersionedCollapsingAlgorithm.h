#pragma once
#include <Processors/Merges/Algorithms/IMergingAlgorithmWithSharedChunks.h>
#include <Processors/Merges/Algorithms/MergedData.h>
#include <Processors/Merges/Algorithms/FixedSizeDequeWithGaps.h>
#include <Processors/Transforms/ColumnGathererTransform.h>
#include <queue>

namespace DB
{

/** Merges several sorted inputs to one.
  * For each group of consecutive identical values of the sorting key
  *   (the columns by which the data is sorted, including specially specified version column),
  *   merges any pair of consecutive rows with opposite sign.
  */
class VersionedCollapsingAlgorithm final : public IMergingAlgorithmWithSharedChunks
{
public:
    /// Don't need version column. It's in primary key.
    VersionedCollapsingAlgorithm(
        const Block & header, size_t num_inputs,
        SortDescription description_, const String & sign_column_,
        size_t max_block_size_rows,
        size_t max_block_size_bytes,
        WriteBuffer * out_row_sources_buf_ = nullptr,
        bool use_average_block_sizes = false);

    const char * getName() const override { return "VersionedCollapsingAlgorithm"; }
    Status merge() override;

private:
    size_t sign_column_number = 0;

    const size_t max_rows_in_queue;

    /// Rows with the same primary key and sign.
    FixedSizeDequeWithGaps<RowRef> current_keys;
    Int8 sign_in_queue = 0;

    std::queue<RowSourcePart> current_row_sources;   /// Sources of rows with the current primary key

    void insertGap(size_t gap_size);
    void insertRow(size_t skip_rows, const RowRef & row);
};

}
