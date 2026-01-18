#pragma once

#include <deque>
#include <vector>
#include <Core/Block_fwd.h>
#include <Core/SortDescription.h>
#include <Processors/Chunk.h>
#include <Processors/IProcessor.h>
#include <Processors/Port.h>
#include <Processors/RowsBeforeStepCounter.h>
#include <base/types.h>

namespace DB
{

/// Implementation for LIMIT N OFFSET M
/// where N and M are fractions in (0, 1) range (non-inclusive) representing percentages.
///
/// This processor supports multiple inputs and outputs (the same number).
/// Each pair of input and output port works independently.
/// The reason to have multiple ports is to be able to stop all sources when limit is reached, in a query like:
///     SELECT * FROM system.numbers_mt WHERE number = 1000000 LIMIT 0.1
///
/// with_ties - implementation of LIMIT WITH TIES. It works only for single port.
///
/// Processor workflow:
/// while input.read():
///     1. read and cache input chunk
///     2. increase total input rows counter
///     3. if offset or fractional_offset, drop from cache
///        chunks that we became 100% sure will be offsetted entirely.
///     4. remove from cache and push to output, chunks that we
///        we becamse 100% sure will be pushed
/// 5. calculate integral limit/offset = (rows_cnt * fraction)
/// 6. apply normal limit/offset logic on remaining cached chunks.
class FractionalLimitTransform final : public IProcessor
{
private:
    Float64 limit_fraction;
    Float64 offset_fraction;

    /// Variables to hold remaining integral limit/offset values to use.
    UInt64 offset = 0; // additionally holds UInt64 offset_ from constructor
    UInt64 limit = 0;

    bool with_ties;
    const SortDescription limit_with_ties_sort_description;

    Chunk previous_row_chunk; /// for WITH TIES, contains only sort columns
    std::vector<size_t> sort_column_positions;

    UInt64 rows_read_from_cache = 0;

    RowsBeforeStepCounterPtr rows_before_limit_at_least;

    /// State of port's pair.
    /// Chunks from different port pairs are not mixed for better cache locality.
    struct PortsData
    {
        Chunk current_chunk;

        InputPort * input_port = nullptr;
        OutputPort * output_port = nullptr;
        bool is_finished = false;
    };

    std::vector<PortsData> ports_data;
    UInt64 num_finished_ports = 0;

    /// Total number of input rows.
    UInt64 rows_cnt = 0;
    /// Number of rows output-ed at pull phase.
    UInt64 outputed_rows_cnt = 0;

    struct CacheEntry
    {
        OutputPort * output_port = nullptr;
        Chunk chunk;
    };
    std::deque<CacheEntry> chunks_cache;

    Chunk makeChunkWithPreviousRow(const Chunk & current_chunk, UInt64 row_num) const;
    ColumnRawPtrs extractSortColumns(const Columns & columns) const;
    bool sortColumnsEqualAt(const ColumnRawPtrs & current_chunk_sort_columns, UInt64 current_chunk_row_num) const;

public:
    FractionalLimitTransform(
        SharedHeader header_,
        Float64 limit_fraction_,
        Float64 offset_fraction_,
        UInt64 offset_ = 0,
        size_t num_streams = 1,
        bool with_ties_ = false,
        SortDescription description_ = {});

    String getName() const override { return "FractionalLimit"; }

    Status prepare(const PortNumbers & /*updated_input_ports*/, const PortNumbers & /*updated_output_ports*/) override;
    Status prepare() override; /// Compatibility for TreeExecutor.
    Status pullData(PortsData & data);
    Status pushData();
    void splitChunk(Chunk & current_chunk);

    InputPort & getInputPort() { return inputs.front(); }
    OutputPort & getOutputPort() { return outputs.front(); }

    void setRowsBeforeLimitCounter(RowsBeforeStepCounterPtr counter) override { rows_before_limit_at_least.swap(counter); }
};

}
