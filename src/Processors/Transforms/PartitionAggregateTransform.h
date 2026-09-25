#pragma once

#include <Common/Arena.h>
#include <Common/HashTable/HashMap.h>
#include <Core/ColumnNumbers.h>
#include <Interpreters/TemporaryDataOnDisk.h>
#include <Interpreters/WindowDescription.h>
#include <Processors/IAccumulatingTransform.h>

namespace DB
{

/** Computes aggregate window functions over whole partitions, `f(x) OVER (PARTITION BY keys)`, without
  * sorting. Keys are grouped by their serialized bytes, so equal keys must have equal bytes (see
  * `keyTypeBreaksHashSharding`). The input is buffered and spills under the external sort thresholds.
  */
class PartitionAggregateTransform final : public IAccumulatingTransform
{
public:
    struct SpillSettings
    {
        /// Spill when the buffered rows exceed this, 0 to never spill.
        size_t max_bytes_before_external = 0;
        /// And the query uses more memory than this, 0 for no condition.
        size_t max_query_bytes_before_external = 0;
        size_t min_free_disk_space = 0;
        TemporaryDataOnDiskScopePtr tmp_data;
    };

    PartitionAggregateTransform(
        SharedHeader input_header,
        SharedHeader output_header,
        ColumnNumbers key_positions_,
        std::vector<WindowFunctionDescription> functions_,
        SpillSettings spill_settings_);

    ~PartitionAggregateTransform() override;

    String getName() const override { return "PartitionAggregateTransform"; }

protected:
    void consume(Chunk chunk) override;
    Chunk generate() override;

private:
    void spill();

    const ColumnNumbers key_positions;
    const std::vector<WindowFunctionDescription> functions;
    const SpillSettings spill_settings;

    std::vector<ColumnNumbers> argument_positions;
    /// The states of all functions of a group are stored together, at these offsets.
    std::vector<size_t> state_offsets;
    size_t total_state_size = 0;
    size_t state_alignment = 1;

    Arena arena;
    HashMapWithSavedHash<std::string_view, UInt32> key_to_group;
    PaddedPODArray<AggregateDataPtr> places;
    PaddedPODArray<AggregateDataPtr> row_places;

    /// Input chunks with the group of every row appended as the last column.
    Chunks chunks;
    size_t chunks_bytes = 0;
    size_t next_chunk = 0;

    /// Constant columns are not written to the temporary file.
    std::vector<bool> is_const_column;
    std::optional<TemporaryBlockStreamHolder> spilled;
    std::optional<TemporaryBlockStreamReaderHolder> spilled_reader;

    Columns results;
    bool results_ready = false;

};

}
