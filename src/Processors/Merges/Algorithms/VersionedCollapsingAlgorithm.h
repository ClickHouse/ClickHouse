#pragma once
#include <Common/QueueWithMemoryTracking.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Processors/Merges/Algorithms/IMergingAlgorithmWithSharedChunks.h>
#include <Processors/Merges/Algorithms/MergedData.h>
#include <Processors/Merges/Algorithms/FixedSizeDequeWithGaps.h>
#include <Processors/Transforms/ColumnGathererTransform.h>

namespace DB
{

/** Merges several sorted inputs to one.
  * For each group of consecutive identical values of the sorting key
  *   (the columns by which the data is sorted, including specially specified version column),
  *   merges any pair of consecutive rows with opposite sign.
  * Rows whose sign is neither 1 nor -1 are kept as is.
  */
class VersionedCollapsingAlgorithm final : public IMergingAlgorithmWithSharedChunks
{
public:
    /// Don't need version column. It's in primary key.
    VersionedCollapsingAlgorithm(
        SharedHeader header, size_t num_inputs,
        SortDescription description_, const String & sign_column_,
        size_t max_block_size_rows,
        size_t max_block_size_bytes,
        std::optional<size_t> max_dynamic_subcolumns_,
        WriteBuffer * out_row_sources_buf_ = nullptr,
        bool use_average_block_sizes = false);

    [[nodiscard]] const char * getName() const override { return "VersionedCollapsingAlgorithm"; }
    Status merge() override;

private:
    size_t sign_column_number = 0;

    const size_t max_rows_in_queue;

    /// Rows with the same primary key and sign.
    FixedSizeDequeWithGaps<RowRef> current_keys;
    Int8 sign_in_queue = 0;

    QueueWithMemoryTracking<RowSourcePart> current_row_sources;   /// Sources of rows with the current primary key

    /// Rows whose sign is neither 1 nor -1 that a vertical merge keeps, copied by value (a `RowRef` would pin a
    /// shared chunk) until the rows read before them are emitted.
    MutableColumns invalid_sign_columns;
    VectorWithMemoryTracking<size_t> invalid_sign_block_sizes;
    size_t next_invalid_sign_row = 0;

    void insertGap(size_t gap_size);
    void insertRow(size_t skip_rows, const RowRef & row);
    void bufferInvalidSignRow(const RowRef & row);
    void insertBufferedInvalidSignRow();
};

}
