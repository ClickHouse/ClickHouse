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
/// where N and M are fractions in (0, 1) representing percentages.
///
/// This processor support multiple inputs and outputs (the same number).
/// Each pair of input and output port works independently.
/// The reason to have multiple ports is to be able to stop all sources when limit is reached, in a query like:
///     SELECT * FROM system.numbers_mt WHERE number = 1000000 LIMIT 0.1
///
/// with_ties - implementation of LIMIT WITH TIES. It works only for single port.
class FractionalLimitTransform final : public IProcessor
{
private:
    Float64 limit_fraction;
    Float64 offset_fraction;

    // Variables to hold real LIMIT and OFFSET values to
    // use after (input_rows_cnt * fraction) calculation.
    UInt64 offset = 0; // additionally hold UInt64 offset_ from constructor
    UInt64 limit = 0;

    bool with_ties;
    const SortDescription limit_with_ties_sort_description;

    Chunk previous_row_chunk; /// for WITH TIES, contains only sort columns
    std::vector<size_t> sort_column_positions;

    UInt64 rows_read = 0; /// including the last read block
    RowsBeforeStepCounterPtr rows_before_limit_at_least;

    /// State of port's pair.
    /// Chunks from different port pairs are not mixed for better cache locality.
    struct PortsData
    {
        Chunk current_chunk;

        InputPort * input_port = nullptr;
        OutputPort * output_port = nullptr;
        bool is_input_port_finished = false;
    };

    std::vector<PortsData> ports_data;
    size_t num_finished_input_ports = 0;

    /// Processor workflow:
    /// 1. read and cache all input chunks (with their output destination)
    /// 2. get total rows count from input
    /// 3. calculate target limit, offset
    /// 4. apply normal limit, offset logic on cached data.
    size_t rows_cnt = 0;
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
